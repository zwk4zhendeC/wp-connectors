use anyhow::Result;
use serde_json::{Value, json};
use wp_connector_api::ParamMap;

pub const TEST_HTTP_SERVER: &str = "http://127.0.0.1:8080";
pub const ALL_HTTP_FORMATS: [&str; 6] = ["json", "ndjson", "csv", "kv", "raw", "proto-text"];

pub fn create_http_test_config(
    path: &str,
    fmt: &str,
    compression: &str,
    username: Option<&str>,
    password: Option<&str>,
    batch_size: usize,
) -> ParamMap {
    let mut params = ParamMap::new();
    params.insert(
        "endpoint".into(),
        json!(format!("{}{}", TEST_HTTP_SERVER, path)),
    );
    params.insert("method".into(), json!("POST"));
    params.insert("fmt".into(), json!(fmt));
    params.insert("batch_size".into(), json!(batch_size));
    params.insert("timeout_secs".into(), json!(30));
    params.insert("max_retries".into(), json!(3));
    params.insert("compression".into(), json!(compression));

    if let Some(username) = username {
        params.insert("username".into(), json!(username));
    }
    if let Some(password) = password {
        params.insert("password".into(), json!(password));
    }

    params
}

pub fn create_http_integration_scenarios() -> Vec<(String, ParamMap)> {
    let mut scenarios = Vec::new();

    for fmt in ALL_HTTP_FORMATS {
        scenarios.push((
            format!("basic_{fmt}"),
            create_http_test_config(&format!("/ingest/{fmt}"), fmt, "none", None, None, 1),
        ));
        scenarios.push((
            format!("auth_{fmt}"),
            create_http_test_config(
                &format!("/auth/ingest/{fmt}"),
                fmt,
                "none",
                Some("root"),
                Some("root"),
                1,
            ),
        ));
        scenarios.push((
            format!("gzip_{fmt}"),
            create_http_test_config(&format!("/gzip/ingest/{fmt}"), fmt, "gzip", None, None, 1),
        ));
    }

    scenarios
}

pub async fn wait_for_http_ready() -> Result<()> {
    let client = reqwest::Client::new();
    let mut last_error = None;

    for attempt in 1..=10 {
        match client
            .get(format!("{}/health", TEST_HTTP_SERVER))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                println!("✓ HTTP 测试服务已就绪，第 {} 次探测成功", attempt);
                return Ok(());
            }
            Ok(resp) => last_error = Some(format!("unexpected status: {}", resp.status())),
            Err(err) => last_error = Some(err.to_string()),
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }

    anyhow::bail!(
        "等待 HTTP 测试服务就绪超时: {}",
        last_error.unwrap_or_else(|| "未知错误".to_string())
    )
}

pub async fn query_http_count(params: ParamMap) -> Result<i64> {
    let fmt = params
        .get("fmt")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("http test params missing fmt"))?;

    let value: Value = reqwest::get(format!("{}/count", TEST_HTTP_SERVER))
        .await?
        .json()
        .await?;

    value
        .get("counts")
        .and_then(|counts| counts.get(fmt))
        .and_then(Value::as_i64)
        .ok_or_else(|| anyhow::anyhow!("http count response missing counts.{}", fmt))
}
