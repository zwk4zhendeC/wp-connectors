use std::time::Duration;

use async_trait::async_trait;
use orion_conf::StructError;
use prometheus::{Encoder, TextEncoder};
use std::sync::Arc;
use sysinfo::System;
use tokio::{sync::oneshot, task::JoinHandle};
use wp_connector_api::{SinkReason, SinkResult};
use wp_log::{error_data, info_data};
use wp_model_core::model::{DataRecord, Value};

use crate::victoriametrics::metrics::{
    cpu_usage_stat, memory_usage_stat, sink_type_stat, source_type_stat,
};

use super::metrics::{parse_all_stat, receive_data_stat, sink_stat};

pub(crate) struct VictoriaMetricExporter {
    insert_url: String,
    client: reqwest::Client,
    flush_interval: Duration,
    stop_tx: Option<oneshot::Sender<()>>,
    flush_handle: Option<JoinHandle<()>>,
    system: System,
}

impl Clone for VictoriaMetricExporter {
    fn clone(&self) -> Self {
        Self {
            system: System::new(),
            insert_url: self.insert_url.clone(),
            client: self.client.clone(),
            flush_interval: self.flush_interval,
            stop_tx: None,
            flush_handle: None,
        }
    }
}

impl VictoriaMetricExporter {
    pub(crate) fn new(
        insert_url: String,
        client: reqwest::Client,
        flush_interval: Duration,
    ) -> Self {
        Self {
            insert_url,
            flush_interval,
            stop_tx: None,
            flush_handle: None,
            client,
            system: System::new(),
        }
    }

    pub(crate) async fn save_metric_to_victoriametric(&self) -> SinkResult<()> {
        Self::push_metrics(&self.client, &self.insert_url).await
    }

    pub(crate) fn start_flush_task(&mut self) {
        if self.flush_interval.is_zero() {
            error_data!("VictoriaMetric flush interval is zero; skip scheduling.");
            return;
        }
        let (stop_tx, mut stop_rx) = oneshot::channel();
        let runner = self.clone();
        let interval = self.flush_interval;
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        if let Err(err) = runner.save_metric_to_victoriametric().await {
                            error_data!("VictoriaMetric periodic push failed: {}", err);
                        }
                    }
                    _ = &mut stop_rx => break,
                }
            }
        });
        self.stop_tx = Some(stop_tx);
        self.flush_handle = Some(handle);
    }

    async fn stop_flush_task(&mut self) {
        if let Err(err) = self.save_metric_to_victoriametric().await {
            error_data!("VictoriaMetric periodic push failed: {}", err);
        }
        self.stop_now();
        if let Some(handle) = self.flush_handle.take()
            && let Err(err) = handle.await
        {
            error_data!("VictoriaMetric flush task join error: {}", err);
        }
    }

    fn stop_now(&mut self) {
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(());
        }
        if let Some(handle) = self.flush_handle.take() {
            handle.abort();
            self.flush_handle = None;
        }
    }

    async fn push_metrics(client: &reqwest::Client, insert_url: &str) -> SinkResult<()> {
        let encoder = TextEncoder::new();
        let metric_families = prometheus::gather();
        if metric_families.is_empty() {
            info_data!("No metrics to export");
            return Ok(());
        }
        let mut buffer = Vec::new();
        if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
            return Err(
                StructError::from(SinkReason::Sink("prometheus encode error".to_string()))
                    .with_detail(e.to_string()),
            );
        }

        let response = client
            .post(insert_url)
            .body(buffer)
            .send()
            .await
            .map_err(|e| {
                StructError::from(SinkReason::Sink("reqwest send error".to_string()))
                    .with_detail(e.to_string())
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            info_data!("VictoriaMetrics API error: {} - {}", status, body);
            return Err(StructError::from(SinkReason::Sink(format!(
                "VictoriaMetrics API error: {} - {}",
                status, body
            ))));
        }
        Ok(())
    }
}

#[async_trait]
impl wp_connector_api::AsyncRecordSink for VictoriaMetricExporter {
    async fn sink_record(&mut self, data: &DataRecord) -> SinkResult<()> {
        if let Some(Value::Chars(field)) = data.get2("stage").map(|x| x.get_value()) {
            match field.as_str() {
                "Pick" => {
                    receive_data_stat(data);
                    source_type_stat(data);
                }
                "Parse" => {
                    // parse_success_stat(data);
                    parse_all_stat(data);
                }
                "Sink" => {
                    sink_stat(data);
                    sink_type_stat(data);
                }
                _ => {}
            }
        }
        cpu_usage_stat(data, &mut self.system);
        memory_usage_stat(data, &mut self.system);
        Ok(())
    }

    async fn sink_records(&mut self, data: Vec<Arc<DataRecord>>) -> SinkResult<()> {
        for record in data {
            self.sink_record(record.as_ref()).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl wp_connector_api::AsyncCtrl for VictoriaMetricExporter {
    async fn stop(&mut self) -> SinkResult<()> {
        self.stop_flush_task().await;
        Ok(())
    }
    async fn reconnect(&mut self) -> SinkResult<()> {
        Ok(())
    }
}

#[async_trait]
impl wp_connector_api::AsyncRawDataSink for VictoriaMetricExporter {
    async fn sink_str(&mut self, _data: &str) -> SinkResult<()> {
        Err(SinkReason::Sink(
            "VictoriaMetric exporter does not support raw input; route TDC metrics only".into(),
        )
        .into())
    }

    async fn sink_bytes(&mut self, _data: &[u8]) -> SinkResult<()> {
        Err(SinkReason::Sink(
            "VictoriaMetric exporter does not support raw bytes; route TDC metrics only".into(),
        )
        .into())
    }

    async fn sink_str_batch(&mut self, _data: Vec<&str>) -> SinkResult<()> {
        Err(SinkReason::Sink(
            "VictoriaMetric exporter does not support raw input; route TDC metrics only".into(),
        )
        .into())
    }

    async fn sink_bytes_batch(&mut self, _data: Vec<&[u8]>) -> SinkResult<()> {
        Err(SinkReason::Sink(
            "VictoriaMetric exporter does not support raw bytes; route TDC metrics only".into(),
        )
        .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::victoriametrics::metrics::{
        PARSE_ALL, RECV_FROM_SOURCE, SEND_TO_SINK, SINK_TYPES, parse_all, send_sink,
        sink_type_values, source_values,
    };
    use wp_connector_api::{AsyncCtrl, AsyncRecordSink};
    use wp_model_core::model::{DataField, DataRecord};

    fn test_exporter() -> VictoriaMetricExporter {
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(1))
            .build()
            .expect("client");
        VictoriaMetricExporter::new(
            "http://127.0.0.1:8428/insert".into(),
            client,
            Duration::from_secs(1),
        )
    }

    /// 测试 sink_record 更新指标
    #[tokio::test]
    async fn sink_record_updates_metrics() {
        let mut exporter = test_exporter();

        // Pick stage
        let pick_key = "pick-target";
        let mut pick_record = DataRecord::default();
        pick_record.append(DataField::from_chars("stage", "Pick"));
        pick_record.append(DataField::from_chars("target", pick_key));
        pick_record.append(DataField::from_digit("total", 2));
        pick_record.append(DataField::from_chars("source_type", "kafka"));
        pick_record.append(DataField::from_chars("access_ip", "127.0.0.1"));
        let (pick_values, _) = source_values(&pick_record);
        let pick_labels = pick_values.values();
        let pick_counter = RECV_FROM_SOURCE.with_label_values(&pick_labels);
        let pick_before = pick_counter.get();
        exporter.sink_record(&pick_record).await.unwrap();
        assert_eq!(pick_counter.get(), pick_before + 2);

        // Parse stage
        let parse_key = "parse-target";
        let mut parse_record = DataRecord::default();
        parse_record.append(DataField::from_chars("stage", "Parse"));
        parse_record.append(DataField::from_chars("target", parse_key));
        parse_record.append(DataField::from_digit("success", 5));
        parse_record.append(DataField::from_digit("total", 5));
        parse_record.append(DataField::from_chars("package_name", "pkg-a"));
        parse_record.append(DataField::from_chars("rule_name", parse_key));
        let (parse_values, _) = parse_all(&parse_record);
        let parse_labels = parse_values.values();
        let parse_all_counter = PARSE_ALL.with_label_values(&parse_labels);
        let parse_all_before = parse_all_counter.get();
        exporter.sink_record(&parse_record).await.unwrap();
        assert_eq!(parse_all_counter.get(), parse_all_before + 5);

        // Sink stage
        let sink_name = "sink-target";
        let sink_category = "sink-category";
        let sink_business = "sink-business";
        let log_business = "log-biz";
        let mut sink_record_data = DataRecord::default();
        sink_record_data.append(DataField::from_chars("stage", "Sink"));
        sink_record_data.append(DataField::from_chars("target", sink_name)); // 使用 sink_name
        sink_record_data.append(DataField::from_chars("sink_category", sink_category));
        sink_record_data.append(DataField::from_chars("sink_business", sink_business));
        sink_record_data.append(DataField::from_chars("log_business", log_business));
        sink_record_data.append(DataField::from_digit("success", 1));
        sink_record_data.append(DataField::from_chars("sink_group", sink_business));
        let (sink_values, _) = send_sink(&sink_record_data);
        let sink_labels = sink_values.values();
        let sink_counter = SEND_TO_SINK.with_label_values(&sink_labels);
        let sink_before = sink_counter.get();
        let (sink_type_metrics, _) = sink_type_values(&sink_record_data);
        let sink_type_labels = sink_type_metrics.values();
        let sink_gauge = SINK_TYPES.with_label_values(&sink_type_labels);
        sink_gauge.set(0.0);
        exporter.sink_record(&sink_record_data).await.unwrap();
        assert_eq!(sink_counter.get(), sink_before + 1);
        assert_eq!(sink_gauge.get(), 1.0);
    }

    #[tokio::test]
    async fn flush_task_start_and_stop_transitions() {
        let mut exporter = test_exporter();
        assert!(exporter.flush_handle.is_none());
        exporter.start_flush_task();
        assert!(exporter.flush_handle.is_some());
        assert!(exporter.stop_tx.is_some());
        exporter.stop().await.unwrap();
        assert!(exporter.flush_handle.is_none());
        assert!(exporter.stop_tx.is_none());
    }
}
