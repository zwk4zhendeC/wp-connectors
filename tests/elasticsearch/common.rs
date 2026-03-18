use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::{Value, json};
use wp_connector_api::ParamMap;

pub const TEST_ES_PROTOCOL: &str = "http";
pub const TEST_ES_HOST: &str = "127.0.0.1";
pub const TEST_ES_PORT: u16 = 9200;
pub const TEST_ES_INDEX: &str = "wp_nginx";
pub const TEST_ES_USER: &str = "elastic";
pub const TEST_ES_PASSWORD: &str = "zgVClXP2";
const ELASTICSEARCH_READY_ATTEMPTS: usize = 20;
const ELASTICSEARCH_READY_INTERVAL_SECS: u64 = 2;
const ELASTICSEARCH_READY_STABLE_PROBES: usize = 3;

fn es_endpoint() -> String {
    format!("{}://{}:{}", TEST_ES_PROTOCOL, TEST_ES_HOST, TEST_ES_PORT)
}

fn es_client() -> Client {
    Client::new()
}

pub fn create_elasticsearch_test_config() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("protocol".into(), json!(TEST_ES_PROTOCOL));
    params.insert("host".into(), json!(TEST_ES_HOST));
    params.insert("port".into(), json!(TEST_ES_PORT));
    params.insert("index".into(), json!(TEST_ES_INDEX));
    params.insert("username".into(), json!(TEST_ES_USER));
    params.insert("password".into(), json!(TEST_ES_PASSWORD));
    params.insert("timeout_secs".into(), json!(60));
    params.insert("max_retries".into(), json!(3));
    params
}

async fn es_request(method: reqwest::Method, path: &str) -> Result<reqwest::Response> {
    es_client()
        .request(method, format!("{}{}", es_endpoint(), path))
        .basic_auth(TEST_ES_USER, Some(TEST_ES_PASSWORD))
        .send()
        .await
        .with_context(|| format!("请求 Elasticsearch 失败: {}", path))
}

async fn probe_elasticsearch_service_ready() -> Result<()> {
    let resp = es_request(
        reqwest::Method::GET,
        "/_cluster/health?wait_for_status=yellow&timeout=1s",
    )
    .await?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        anyhow::bail!("status={}, body={}", status, body);
    }

    Ok(())
}

async fn probe_elasticsearch_index_ddl() -> Result<()> {
    let probe_index = "__wp_ready_probe";

    let delete_resp = es_request(reqwest::Method::DELETE, &format!("/{}", probe_index)).await?;
    if !(delete_resp.status().is_success() || delete_resp.status().as_u16() == 404) {
        let status = delete_resp.status();
        let body = delete_resp.text().await.unwrap_or_default();
        anyhow::bail!("删除 Elasticsearch 探针索引失败: status={}, body={}", status, body);
    }

    let create_resp = es_client()
        .put(format!("{}/{}", es_endpoint(), probe_index))
        .basic_auth(TEST_ES_USER, Some(TEST_ES_PASSWORD))
        .json(&json!({
            "settings": {"number_of_shards": 1, "number_of_replicas": 0}
        }))
        .send()
        .await
        .context("创建 Elasticsearch 探针索引失败")?;
    if !create_resp.status().is_success() {
        let status = create_resp.status();
        let body = create_resp.text().await.unwrap_or_default();
        anyhow::bail!("创建 Elasticsearch 探针索引失败: status={}, body={}", status, body);
    }

    let cleanup_resp = es_request(reqwest::Method::DELETE, &format!("/{}", probe_index)).await?;
    if !cleanup_resp.status().is_success() {
        let status = cleanup_resp.status();
        let body = cleanup_resp.text().await.unwrap_or_default();
        anyhow::bail!("清理 Elasticsearch 探针索引失败: status={}, body={}", status, body);
    }

    Ok(())
}

pub async fn wait_for_elasticsearch_ready() -> Result<()> {
    let mut last_error = None;
    let mut stable_successes = 0usize;

    for attempt in 1..=ELASTICSEARCH_READY_ATTEMPTS {
        match probe_elasticsearch_service_ready().await {
            Ok(()) => {
                stable_successes += 1;
                if stable_successes >= ELASTICSEARCH_READY_STABLE_PROBES {
                    println!(
                        "✓ Elasticsearch 服务已稳定就绪，连续 {} 次探测成功（第 {} 次完成）",
                        ELASTICSEARCH_READY_STABLE_PROBES, attempt
                    );
                    return Ok(());
                }

                println!(
                    "Elasticsearch 服务探测成功，继续观察稳定性（{}/{})...",
                    stable_successes, ELASTICSEARCH_READY_STABLE_PROBES
                );
            }
            Err(err) => {
                stable_successes = 0;
                last_error = Some(err.to_string());
            }
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(ELASTICSEARCH_READY_INTERVAL_SECS))
            .await;
    }

    anyhow::bail!(
        "等待 Elasticsearch 就绪超时: {}",
        last_error.unwrap_or_else(|| "未知错误".to_string())
    )
}

pub async fn init_elasticsearch_index() -> Result<()> {
    wait_for_elasticsearch_ready().await?;

    let delete_resp = es_request(reqwest::Method::DELETE, &format!("/{}", TEST_ES_INDEX)).await?;
    if !(delete_resp.status().is_success() || delete_resp.status().as_u16() == 404) {
        let status = delete_resp.status();
        let body = delete_resp.text().await.unwrap_or_default();
        anyhow::bail!(
            "删除 Elasticsearch 索引失败: status={}, body={}",
            status,
            body
        );
    }

    let create_resp = es_client()
        .put(format!("{}/{}", es_endpoint(), TEST_ES_INDEX))
        .basic_auth(TEST_ES_USER, Some(TEST_ES_PASSWORD))
        .json(&json!({
            "settings": {"number_of_shards": 1, "number_of_replicas": 0}
        }))
        .send()
        .await
        .context("创建 Elasticsearch 索引失败")?;
    if !create_resp.status().is_success() {
        let status = create_resp.status();
        let body = create_resp.text().await.unwrap_or_default();
        anyhow::bail!(
            "创建 Elasticsearch 索引失败: status={}, body={}",
            status,
            body
        );
    }

    println!("✓ Elasticsearch 测试索引初始化完成");
    Ok(())
}

pub async fn query_index_count() -> Result<i64> {
    let resp = es_request(reqwest::Method::GET, &format!("/{}/_count", TEST_ES_INDEX)).await?;
    let status = resp.status();
    let body = resp.text().await?;
    if !status.is_success() {
        anyhow::bail!(
            "查询 Elasticsearch count 失败: status={}, body={}",
            status,
            body
        );
    }

    let value: Value = serde_json::from_str(&body)
        .with_context(|| format!("解析 Elasticsearch count 响应失败: {}", body))?;
    value
        .get("count")
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow::anyhow!("Elasticsearch count 响应缺少 count 字段: {}", body))
}

pub async fn wait_for_elasticsearch_sink_ready() -> Result<()> {
    let mut last_error = None;
    let mut stable_successes = 0usize;

    for attempt in 1..=ELASTICSEARCH_READY_ATTEMPTS {
        match wait_for_elasticsearch_ready().await {
            Ok(()) => match probe_elasticsearch_index_ddl().await {
                Ok(_) => {
                    stable_successes += 1;
                    if stable_successes >= ELASTICSEARCH_READY_STABLE_PROBES {
                        println!(
                            "✓ Elasticsearch sink 已稳定就绪，连续 {} 次探测成功（第 {} 次完成）",
                            ELASTICSEARCH_READY_STABLE_PROBES, attempt
                        );
                        return Ok(());
                    }

                    println!(
                        "Elasticsearch sink 探测成功，继续观察稳定性（{}/{})...",
                        stable_successes, ELASTICSEARCH_READY_STABLE_PROBES
                    );
                }
                Err(err) => {
                    stable_successes = 0;
                    last_error = Some(err.to_string());
                }
            },
            Err(err) => {
                stable_successes = 0;
                last_error = Some(err.to_string());
            }
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(ELASTICSEARCH_READY_INTERVAL_SECS))
            .await;
    }

    anyhow::bail!(
        "等待 Elasticsearch sink 就绪超时: {}",
        last_error.unwrap_or_else(|| "未知错误".to_string())
    )
}
