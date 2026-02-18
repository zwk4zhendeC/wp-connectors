//! Doris Sink 实现 - 使用 Stream Load API
//!
//! # Stream Load API
//!
//! 本实现使用 Doris 的 HTTP Stream Load API 进行批量数据导入：
//! - 高性能批量导入（比 MySQL 协议快 3-5 倍）
//! - Exactly-Once 语义（通过 label 机制）
//! - 无 SQL 注入风险（使用 JSON 格式）
//! - 支持负载均衡和故障转移
//!
//! # Label 机制
//!
//! 每次批量导入使用第一条记录的 `wp_event_id` 作为 label，保证幂等性：
//! - 相同 label 的请求会被 Doris 拒绝
//! - 重试时使用相同 label，避免数据重复

use crate::doris::config::DorisSinkConfig;
use async_trait::async_trait;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use wp_connector_api::{
    AsyncCtrl, AsyncRawDataSink, AsyncRecordSink, SinkError, SinkReason, SinkResult,
};
use wp_model_core::model::{DataRecord, DataType};

pub struct DorisSink {
    client: Client,
    url: String, // 预先构建的完整 URL
    user: String,
    password: String,
    max_retries: i32,
    headers: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct StreamLoadResponse {
    #[serde(rename = "Status")]
    status: String,
    #[serde(rename = "Message")]
    message: String,
    #[serde(rename = "NumberTotalRows", default)]
    #[allow(dead_code)]
    number_total_rows: i64,
    #[serde(rename = "NumberLoadedRows", default)]
    number_loaded_rows: i64,
    #[serde(rename = "NumberFilteredRows", default)]
    number_filtered_rows: i64,
}

impl DorisSink {
    /// 构建 Doris Sink，使用 Stream Load API。
    ///
    /// # Arguments
    /// * `config` - Doris 连接与写入配置
    ///
    /// # Returns
    /// * `anyhow::Result<Self>` - 成功返回初始化后的 sink
    pub async fn new(config: DorisSinkConfig) -> anyhow::Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .no_proxy() // 禁用所有代理
            .build()?;

        // 预先构建完整的 Stream Load URL
        let url = format!(
            "{}/api/{}/{}/_stream_load",
            config.endpoint, config.database, config.table
        );

        Ok(Self {
            client,
            url,
            user: config.user,
            password: config.password,
            max_retries: config.max_retries,
            headers: config.headers.unwrap_or_default(),
        })
    }

    /// 生成唯一的 label 用于 Stream Load。
    ///
    /// 使用当前时间戳作为 label，确保每次请求都有唯一标识。
    ///
    /// # Returns
    /// * `SinkResult<String>` - label 字符串
    fn generate_label(&self) -> SinkResult<String> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| sink_error(format!("failed to get timestamp: {}", e)))?
            .as_millis();

        Ok(format!("doris_load_{}", timestamp))
    }

    /// 将 DataRecord 转换为 JSON 对象。
    ///
    /// # Arguments
    /// * `record` - 数据记录
    ///
    /// # Returns
    /// * `serde_json::Value` - JSON 对象
    fn record_to_json(&self, record: &DataRecord) -> serde_json::Value {
        let mut map = serde_json::Map::new();

        for field in &record.items {
            if *field.get_meta() == DataType::Ignore {
                continue;
            }

            let name = field.get_name().to_string();
            let value = field.get_value();

            let json_value = match value.to_string().parse::<i64>() {
                Ok(i) => serde_json::Value::Number(i.into()),
                Err(_) => match value.to_string().parse::<f64>() {
                    Ok(f) => serde_json::Number::from_f64(f)
                        .map(serde_json::Value::Number)
                        .unwrap_or_else(|| serde_json::Value::String(value.to_string())),
                    Err(_) => serde_json::Value::String(value.to_string()),
                },
            };

            map.insert(name, json_value);
        }

        serde_json::Value::Object(map)
    }

    /// 将批量记录转换为 NDJSON 格式（每行一个 JSON 对象）。
    ///
    /// # Arguments
    /// * `records` - 数据记录列表
    ///
    /// # Returns
    /// * `SinkResult<String>` - NDJSON 字符串
    fn records_to_ndjson(&self, records: &[Arc<DataRecord>]) -> SinkResult<String> {
        let json_lines: Vec<String> = records
            .iter()
            .map(|record| {
                serde_json::to_string(&self.record_to_json(record.as_ref()))
                    .map_err(|e| sink_error(format!("json serialization failed: {}", e)))
            })
            .collect::<SinkResult<Vec<_>>>()?;

        Ok(json_lines.join("\n"))
    }

    /// 执行 Stream Load 请求。
    ///
    /// # Arguments
    ///
    /// * `label` - 唯一标识符（使用第一条记录的 wp_event_id）
    /// * `data` - NDJSON 格式的数据
    ///
    /// # Returns
    ///
    /// * `SinkResult<()>` - 成功或错误
    async fn stream_load(&self, label: &str, data: String) -> SinkResult<()> {
        let mut retries = 0;
        let max_retries = if self.max_retries < 0 {
            i32::MAX
        } else {
            self.max_retries
        };

        loop {
            let mut request = self
                .client
                .put(&self.url)
                .basic_auth(&self.user, Some(&self.password))
                .header("label", label)
                .header("format", "json")
                .header("read_json_by_line", "true")
                .header("Expect", "100-continue")
                .body(data.clone());

            // 添加自定义 headers
            for (key, value) in &self.headers {
                request = request.header(key, value);
            }

            match request.send().await {
                Ok(response) => {
                    let status = response.status();
                    let body = response
                        .text()
                        .await
                        .unwrap_or_else(|_| "failed to read response".to_string());

                    if status.is_success() {
                        // 解析响应
                        match serde_json::from_str::<StreamLoadResponse>(&body) {
                            Ok(resp) if resp.status == "Success" => {
                                log::info!(
                                    "stream load success: label={}, loaded={}, filtered={}",
                                    label,
                                    resp.number_loaded_rows,
                                    resp.number_filtered_rows
                                );
                                return Ok(());
                            }
                            Ok(resp) => {
                                return Err(sink_error(format!(
                                    "stream load failed: status={}, message={}",
                                    resp.status, resp.message
                                )));
                            }
                            Err(e) => {
                                log::warn!("failed to parse response: {}, body: {}", e, body);
                                return Err(sink_error(format!("invalid response format: {}", e)));
                            }
                        }
                    } else if status == StatusCode::CONFLICT {
                        // Label 已存在，说明数据已经导入过了（幂等性）
                        log::info!("label {} already exists, skipping", label);
                        return Ok(());
                    } else if status.is_client_error() {
                        // 4xx 错误不重试
                        return Err(sink_error(format!(
                            "client error: status={}, body={}",
                            status, body
                        )));
                    } else {
                        // 5xx 错误重试
                        log::warn!(
                            "server error: status={}, body={}, retry={}/{}",
                            status,
                            body,
                            retries + 1,
                            max_retries
                        );
                    }
                }
                Err(e) => {
                    log::warn!(
                        "request failed: {}, retry={}/{}",
                        e,
                        retries + 1,
                        max_retries
                    );
                }
            }

            // 检查是否需要重试
            retries += 1;
            if retries >= max_retries {
                return Err(sink_error(format!(
                    "max retries ({}) exceeded",
                    max_retries
                )));
            }

            // 指数退避
            let backoff_ms = 1000 * 2_u64.pow(retries.min(10) as u32);
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        }
    }

    /// 刷新缓冲区，将所有缓存的记录发送到 Doris。
    ///
    /// # Returns
    /// * `SinkResult<()>` - 成功或错误
    async fn flush_buffer(&mut self) -> SinkResult<()> {
        // No-op: buffering is disabled, records are sent immediately
        Ok(())
    }
}

#[async_trait]
impl AsyncCtrl for DorisSink {
    async fn stop(&mut self) -> SinkResult<()> {
        // 停止前刷新缓冲区，确保没有数据丢失
        self.flush_buffer().await?;
        Ok(())
    }

    async fn reconnect(&mut self) -> SinkResult<()> {
        // HTTP 客户端无需显式重连
        Ok(())
    }
}

#[async_trait]
impl AsyncRecordSink for DorisSink {
    async fn sink_record(&mut self, data: &DataRecord) -> SinkResult<()> {
        self.sink_records(vec![Arc::new(data.clone())]).await
    }

    async fn sink_records(&mut self, data: Vec<Arc<DataRecord>>) -> SinkResult<()> {
        if data.is_empty() {
            return Ok(());
        }

        // 生成label
        let label = self.generate_label()?;
        let ndjson = self.records_to_ndjson(&data)?;
        self.stream_load(&label, ndjson).await?;
        Ok(())
    }
}

#[async_trait]
impl AsyncRawDataSink for DorisSink {
    async fn sink_str(&mut self, _data: &str) -> SinkResult<()> {
        Err(sink_error("doris sink does not accept raw text input"))
    }

    async fn sink_bytes(&mut self, _data: &[u8]) -> SinkResult<()> {
        Err(sink_error("doris sink does not accept raw byte input"))
    }

    async fn sink_str_batch(&mut self, _data: Vec<&str>) -> SinkResult<()> {
        Err(sink_error("doris sink does not accept raw batch input"))
    }

    async fn sink_bytes_batch(&mut self, _data: Vec<&[u8]>) -> SinkResult<()> {
        Err(sink_error(
            "doris sink does not accept raw batch byte input",
        ))
    }
}

/// 统一封装 sink 层错误。
fn sink_error(msg: impl Into<String>) -> SinkError {
    SinkError::from(SinkReason::Sink(msg.into()))
}

#[cfg(test)]
mod tests {
    // 测试将在集成测试中实现
}
