use anyhow::Result;
use sea_orm::sqlx::{
    self, ConnectOptions,
    mysql::{MySqlConnectOptions, MySqlPoolOptions},
};
use sea_orm::{ConnectOptions as SeaConnectOptions, ConnectionTrait, Database};
use serde_json::json;
use std::str::FromStr;
use wp_connector_api::ParamMap;
use wp_connectors::doris::DorisSinkFactory;

use crate::common::{
    component_tools::DockerComposeTool,
    sink::{
        performance_runtime::{SinkPerformanceConfig, SinkPerformanceRuntime},
        sink_info::SinkInfo,
    },
};

const TEST_DORIS_ENDPOINT: &str = "http://localhost:8040";
const TEST_DORIS_MYSQL_HOST: &str = "127.0.0.1";
const TEST_DORIS_MYSQL_PORT: u16 = 9030;
const TEST_DORIS_DB: &str = "test_db";
const TEST_DORIS_TABLE: &str = "wp_nginx";
const TEST_DORIS_USER: &str = "root";
const TEST_DORIS_PASSWORD: Option<&str> = None;

fn doris_mysql_options(database: Option<&str>) -> Result<MySqlConnectOptions> {
    let mut options = MySqlConnectOptions::from_str(&format!(
        "mysql://{}@{}:{}",
        TEST_DORIS_USER, TEST_DORIS_MYSQL_HOST, TEST_DORIS_MYSQL_PORT
    ))?
    .disable_statement_logging();

    if let Some(password) = TEST_DORIS_PASSWORD {
        options = options.password(password);
    }

    if let Some(database) = database {
        options = options.database(database);
    }

    Ok(options)
}

fn doris_mysql_url(database: Option<&str>) -> String {
    let auth = match TEST_DORIS_PASSWORD {
        Some(password) => format!("{}:{}", TEST_DORIS_USER, password),
        None => TEST_DORIS_USER.to_string(),
    };

    match database {
        Some(database) => format!(
            "mysql://{}@{}:{}/{}",
            auth, TEST_DORIS_MYSQL_HOST, TEST_DORIS_MYSQL_PORT, database
        ),
        None => format!(
            "mysql://{}@{}:{}",
            auth, TEST_DORIS_MYSQL_HOST, TEST_DORIS_MYSQL_PORT
        ),
    }
}

async fn create_doris_admin_conn(database: Option<&str>) -> Result<sea_orm::DatabaseConnection> {
    let mut options = SeaConnectOptions::new(doris_mysql_url(database));
    options
        .max_connections(1)
        .min_connections(1)
        .connect_timeout(std::time::Duration::from_secs(5))
        .acquire_timeout(std::time::Duration::from_secs(5))
        .idle_timeout(std::time::Duration::from_secs(5))
        .max_lifetime(std::time::Duration::from_secs(5))
        .sqlx_logging(false)
        .map_sqlx_mysql_opts(|opt| opt.statement_cache_capacity(0));
    Ok(Database::connect(options).await?)
}

async fn create_doris_pool(database: Option<&str>) -> Result<sqlx::MySqlPool> {
    Ok(MySqlPoolOptions::new()
        .max_connections(1)
        .connect_with(doris_mysql_options(database)?)
        .await?)
}

async fn wait_for_doris_ready() -> Result<()> {
    let mut last_error = None;
    for attempt in 1..=10 {
        match create_doris_pool(None).await {
            Ok(pool) => {
                let ready = async {
                    sqlx::query("SHOW DATABASES").fetch_one(&pool).await?;
                    let backends = sqlx::query("SHOW BACKENDS").fetch_all(&pool).await?;
                    if backends.is_empty() {
                        anyhow::bail!("Doris backend 尚未注册");
                    }
                    Ok::<(), anyhow::Error>(())
                }
                .await;

                pool.close().await;
                match ready {
                    Ok(()) => {
                        println!("✓ Doris FE/BE 已就绪，第 {} 次探测成功", attempt);
                        return Ok(());
                    }
                    Err(err) => last_error = Some(err.to_string()),
                }
            }
            Err(err) => last_error = Some(err.to_string()),
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    }

    anyhow::bail!(
        "等待 Doris FE/BE 就绪超时: {}",
        last_error.unwrap_or_else(|| "未知错误".to_string())
    )
}

fn create_doris_performance_config() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("endpoint".into(), json!(TEST_DORIS_ENDPOINT));
    params.insert("database".into(), json!(TEST_DORIS_DB));
    params.insert("table".into(), json!(TEST_DORIS_TABLE));
    params.insert("user".into(), json!(TEST_DORIS_USER));
    params.insert("password".into(), json!(TEST_DORIS_PASSWORD.unwrap_or("")));
    params.insert("timeout_secs".into(), json!(30));
    params.insert("max_retries".into(), json!(3));
    params
}

async fn init_doris_database() -> Result<()> {
    println!("初始化 Doris 性能测试数据库和表...");
    wait_for_doris_ready().await?;

    let db = create_doris_admin_conn(None).await?;
    db.execute_unprepared(&format!("CREATE DATABASE IF NOT EXISTS {}", TEST_DORIS_DB))
        .await?;
    db.execute_unprepared(&format!(
        "DROP TABLE IF EXISTS {}.{}",
        TEST_DORIS_DB, TEST_DORIS_TABLE
    ))
    .await?;

    let create_table_sql = format!(
        r#"CREATE TABLE {}.{} (
            wp_event_id BIGINT COMMENT '事件唯一ID',
            wp_src_key STRING COMMENT '数据来源表示',
            sip STRING COMMENT '客户端IP',
            `timestamp` STRING COMMENT '原始时间字符串',
            `http/request` STRING COMMENT 'HTTP请求行',
            status SMALLINT COMMENT 'HTTP状态码',
            size INT COMMENT '响应大小(byte)',
            referer STRING COMMENT '来源页面',
            `http/agent` STRING COMMENT 'User-Agent'
        )
        ENGINE=OLAP
        DUPLICATE KEY(wp_event_id)
        DISTRIBUTED BY HASH(wp_event_id) BUCKETS 8
        PROPERTIES ("replication_num" = "1")"#,
        TEST_DORIS_DB, TEST_DORIS_TABLE
    );

    let mut last_error = None;
    for attempt in 1..=5 {
        match db.execute_unprepared(&create_table_sql).await {
            Ok(_) => return Ok(()),
            Err(err) => {
                let err_msg = err.to_string();
                if err_msg.contains("already exists") {
                    return Ok(());
                }
                last_error = Some(err_msg);
                println!("Doris 表创建未就绪，第 {} 次重试...", attempt);
                tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            }
        }
    }

    anyhow::bail!(
        "性能测试表创建失败: {}",
        last_error.unwrap_or_else(|| "未知错误".to_string())
    )
}

#[tokio::test]
#[ignore = "性能测试默认不在 CI 中运行，请手动执行"]
// cargo test --release --package wp-connectors --test performance_tests doris_perf_tests::test_doris_sink_performance --features doris -- --exact --nocapture --ignored
async fn test_doris_sink_performance() -> Result<()> {
    let docker_tool = DockerComposeTool::new("tests/doris/docker-compose.yml")?;
    let sink_info = SinkInfo::new(DorisSinkFactory, create_doris_performance_config())
        .with_async_init(|| async { init_doris_database().await });

    let runtime = SinkPerformanceRuntime::new(
        docker_tool,
        vec![sink_info],
        SinkPerformanceConfig::default(),
    );
    runtime.run().await
}
