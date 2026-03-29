#![allow(dead_code)]

use anyhow::{Context, Result};
use sea_orm::{ConnectionTrait, Database, DatabaseConnection, DbBackend, Statement};
use serde_json::json;
use wp_connector_api::ParamMap;

pub const TEST_POSTGRES_ENDPOINT: &str = "127.0.0.1:5432";
pub const TEST_POSTGRES_DATABASE: &str = "default";
pub const TEST_POSTGRES_TABLE: &str = "wp_events";
pub const TEST_POSTGRES_USER: &str = "root";
pub const TEST_POSTGRES_PASSWORD: &str = "123456";

const POSTGRES_READY_ATTEMPTS: usize = 20;
const POSTGRES_READY_INTERVAL_SECS: u64 = 2;
const POSTGRES_READY_STABLE_PROBES: usize = 3;

fn postgres_url() -> String {
    format!(
        "postgres://{}:{}@{}/{}",
        TEST_POSTGRES_USER, TEST_POSTGRES_PASSWORD, TEST_POSTGRES_ENDPOINT, TEST_POSTGRES_DATABASE
    )
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

async fn postgres_connection() -> Result<DatabaseConnection> {
    Database::connect(postgres_url())
        .await
        .context("连接 PostgreSQL 失败")
}

pub fn create_postgresql_test_config() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("endpoint".into(), json!(TEST_POSTGRES_ENDPOINT));
    params.insert("database".into(), json!(TEST_POSTGRES_DATABASE));
    params.insert("username".into(), json!(TEST_POSTGRES_USER));
    params.insert("password".into(), json!(TEST_POSTGRES_PASSWORD));
    params.insert("table".into(), json!(TEST_POSTGRES_TABLE));
    params.insert(
        "columns".into(),
        json!([
            "wp_event_id",
            "wp_src_key",
            "sip",
            "timestamp",
            "http/request",
            "status",
            "size",
            "referer",
            "http/agent"
        ]),
    );
    params.insert("batch_size".into(), json!(5_000));
    params
}

async fn execute_sql(sql: &str) -> Result<()> {
    let db = postgres_connection().await?;
    db.execute_unprepared(sql)
        .await
        .with_context(|| format!("执行 PostgreSQL SQL 失败: {sql}"))?;
    Ok(())
}

async fn query_i64(sql: &str) -> Result<i64> {
    let db = postgres_connection().await?;
    let stmt = Statement::from_string(DbBackend::Postgres, sql.to_string());
    let row = db
        .query_one(stmt)
        .await
        .with_context(|| format!("执行 PostgreSQL 查询失败: {sql}"))?
        .ok_or_else(|| anyhow::anyhow!("PostgreSQL 查询未返回结果: {sql}"))?;

    row.try_get_by_index(0)
        .with_context(|| format!("读取 PostgreSQL 第一列失败: {sql}"))
}

async fn probe_postgres_service_ready() -> Result<()> {
    let value = query_i64("SELECT 1::BIGINT").await?;
    if value != 1 {
        anyhow::bail!("PostgreSQL 探测返回异常值: {value}");
    }
    Ok(())
}

async fn probe_postgres_table_ddl() -> Result<()> {
    execute_sql(
        "DROP TABLE IF EXISTS __wp_ready_probe; \
         CREATE TABLE __wp_ready_probe (id BIGINT PRIMARY KEY); \
         DROP TABLE __wp_ready_probe;",
    )
    .await
}

pub async fn wait_for_postgresql_ready() -> Result<()> {
    let mut last_error = None;
    let mut stable_successes = 0usize;

    for attempt in 1..=POSTGRES_READY_ATTEMPTS {
        match probe_postgres_service_ready().await {
            Ok(()) => {
                stable_successes += 1;
                if stable_successes >= POSTGRES_READY_STABLE_PROBES {
                    println!(
                        "✓ PostgreSQL 服务已稳定就绪，连续 {} 次探测成功（第 {} 次完成）",
                        POSTGRES_READY_STABLE_PROBES, attempt
                    );
                    return Ok(());
                }

                println!(
                    "PostgreSQL 服务探测成功，继续观察稳定性（{}/{})...",
                    stable_successes, POSTGRES_READY_STABLE_PROBES
                );
            }
            Err(err) => {
                stable_successes = 0;
                last_error = Some(err.to_string());
            }
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(
            POSTGRES_READY_INTERVAL_SECS,
        ))
        .await;
    }

    anyhow::bail!(
        "等待 PostgreSQL 就绪超时: {}",
        last_error.unwrap_or_else(|| "未知错误".to_string())
    )
}

pub async fn wait_for_postgresql_sink_ready() -> Result<()> {
    wait_for_postgresql_ready().await?;

    let mut last_error = None;
    let mut stable_successes = 0usize;

    for attempt in 1..=POSTGRES_READY_ATTEMPTS {
        match probe_postgres_table_ddl().await {
            Ok(()) => {
                stable_successes += 1;
                if stable_successes >= POSTGRES_READY_STABLE_PROBES {
                    println!(
                        "✓ PostgreSQL sink 已稳定就绪，连续 {} 次探测成功（第 {} 次完成）",
                        POSTGRES_READY_STABLE_PROBES, attempt
                    );
                    return Ok(());
                }

                println!(
                    "PostgreSQL sink 探测成功，继续观察稳定性（{}/{})...",
                    stable_successes, POSTGRES_READY_STABLE_PROBES
                );
            }
            Err(err) => {
                stable_successes = 0;
                last_error = Some(err.to_string());
            }
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(
            POSTGRES_READY_INTERVAL_SECS,
        ))
        .await;
    }

    anyhow::bail!(
        "等待 PostgreSQL sink 就绪超时: {}",
        last_error.unwrap_or_else(|| "未知错误".to_string())
    )
}

pub async fn init_postgresql_database() -> Result<()> {
    let table = quote_ident(TEST_POSTGRES_TABLE);
    execute_sql(&format!(
        "DROP TABLE IF EXISTS {table};\
         CREATE TABLE {table} (\
             \"wp_event_id\" BIGINT PRIMARY KEY,\
             \"wp_src_key\" TEXT NOT NULL,\
             \"sip\" TEXT NOT NULL,\
             \"timestamp\" TEXT NOT NULL,\
             \"http/request\" TEXT,\
             \"status\" INTEGER NOT NULL,\
             \"size\" BIGINT NOT NULL,\
             \"referer\" TEXT,\
             \"http/agent\" TEXT\
         );"
    ))
    .await?;

    println!("✓ PostgreSQL 测试库表初始化完成");
    Ok(())
}

pub async fn query_table_count() -> Result<i64> {
    query_i64(&format!(
        "SELECT COUNT(*)::BIGINT FROM {}",
        quote_ident(TEST_POSTGRES_TABLE)
    ))
    .await
}
