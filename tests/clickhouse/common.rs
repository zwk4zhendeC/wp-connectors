use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::json;
use wp_connector_api::ParamMap;

pub const TEST_CLICKHOUSE_ENDPOINT: &str = "http://127.0.0.1:8123";
pub const TEST_CLICKHOUSE_DB: &str = "test_db";
pub const TEST_CLICKHOUSE_TABLE: &str = "wp_nginx";
pub const TEST_CLICKHOUSE_USER: &str = "default";
pub const TEST_CLICKHOUSE_PASSWORD: &str = "default";

fn clickhouse_client() -> Client {
    Client::new()
}

pub fn create_clickhouse_test_config() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("endpoint".into(), json!(TEST_CLICKHOUSE_ENDPOINT));
    params.insert("database".into(), json!(TEST_CLICKHOUSE_DB));
    params.insert("table".into(), json!(TEST_CLICKHOUSE_TABLE));
    params.insert("username".into(), json!(TEST_CLICKHOUSE_USER));
    params.insert("password".into(), json!(TEST_CLICKHOUSE_PASSWORD));
    params.insert("timeout_secs".into(), json!(30));
    params.insert("max_retries".into(), json!(3));
    params
}

async fn execute_sql(sql: &str) -> Result<String> {
    let response = clickhouse_client()
        .post(format!("{}/", TEST_CLICKHOUSE_ENDPOINT))
        .basic_auth(TEST_CLICKHOUSE_USER, Some(TEST_CLICKHOUSE_PASSWORD))
        .body(sql.to_owned())
        .send()
        .await
        .with_context(|| format!("执行 ClickHouse SQL 失败: {sql}"))?;

    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        anyhow::bail!("ClickHouse SQL 执行失败: status={}, body={}", status, body);
    }

    Ok(body)
}

pub async fn wait_for_clickhouse_ready() -> Result<()> {
    let mut last_error = None;

    for attempt in 1..=30 {
        match clickhouse_client()
            .get(format!("{}/ping", TEST_CLICKHOUSE_ENDPOINT))
            .basic_auth(TEST_CLICKHOUSE_USER, Some(TEST_CLICKHOUSE_PASSWORD))
            .send()
            .await
        {
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                if status.is_success() && body.trim() == "Ok." {
                    match execute_sql("SELECT 1").await {
                        Ok(_) => {
                            println!("✓ ClickHouse 已就绪，第 {} 次探测成功", attempt);
                            return Ok(());
                        }
                        Err(err) => last_error = Some(err.to_string()),
                    }
                } else {
                    last_error = Some(format!("status={}, body={}", status, body));
                }
            }
            Err(err) => last_error = Some(err.to_string()),
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    }

    anyhow::bail!(
        "等待 ClickHouse 就绪超时: {}",
        last_error.unwrap_or_else(|| "未知错误".to_string())
    )
}

pub async fn init_clickhouse_database() -> Result<()> {
    wait_for_clickhouse_ready().await?;

    execute_sql(&format!(
        "CREATE DATABASE IF NOT EXISTS {}",
        TEST_CLICKHOUSE_DB
    ))
    .await?;
    execute_sql(&format!(
        "DROP TABLE IF EXISTS {}.{}",
        TEST_CLICKHOUSE_DB, TEST_CLICKHOUSE_TABLE
    ))
    .await?;

    execute_sql(&format!(
        "CREATE TABLE {}.{} (\
            wp_event_id Int64, \
            wp_src_key String, \
            sip String, \
            timestamp String, \
            `http/request` String, \
            status Int32, \
            size Int64, \
            referer String, \
            `http/agent` String\
        ) ENGINE = MergeTree ORDER BY wp_event_id",
        TEST_CLICKHOUSE_DB, TEST_CLICKHOUSE_TABLE
    ))
    .await?;

    println!("✓ ClickHouse 测试库表初始化完成");
    Ok(())
}

pub async fn query_table_count() -> Result<i64> {
    let body = execute_sql(&format!(
        "SELECT count() FROM {}.{}",
        TEST_CLICKHOUSE_DB, TEST_CLICKHOUSE_TABLE
    ))
    .await?;

    body.trim()
        .parse::<i64>()
        .with_context(|| format!("解析 ClickHouse count 失败: {}", body.trim()))
}

pub async fn wait_for_clickhouse_sink_ready() -> Result<()> {
    let mut last_error = None;
    let mut consecutive_successes = 0;

    for _attempt in 1..=15 {
        match wait_for_clickhouse_ready().await {
            Ok(()) => match query_table_count().await {
                Ok(_) => {
                    consecutive_successes += 1;
                    if consecutive_successes >= 3 {
                        println!(
                            "✓ ClickHouse sink 已连续{}次探测成功",
                            consecutive_successes
                        );
                        return Ok(());
                    }
                }
                Err(err) => {
                    consecutive_successes = 0;
                    last_error = Some(err.to_string());
                }
            },
            Err(err) => {
                consecutive_successes = 0;
                last_error = Some(err.to_string());
            }
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    }

    anyhow::bail!(
        "等待 ClickHouse sink 就绪超时: {}",
        last_error.unwrap_or_else(|| "未知错误".to_string())
    )
}
