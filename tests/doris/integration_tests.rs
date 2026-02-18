#![cfg(feature = "doris")]
//! Integration tests for Doris sink factory. These tests focus on config validation so they
//! don't require a running Doris cluster.

use serde_json::Value;
use std::collections::BTreeMap;
use wp_connector_api::{AsyncCtrl, AsyncRecordSink, SinkFactory, SinkSpec};
use wp_connectors::doris::{DorisSink, DorisSinkConfig, DorisSinkFactory};
use wp_model_core::model::{DataField, DataRecord};

const TEST_DORIS_ENDPOINT: &str = "http://localhost:8040";
const TEST_DORIS_DB: &str = "test_db";
const TEST_DORIS_USER: &str = "root";
const TEST_DORIS_PASSWORD: &str = "";
const TEST_DORIS_TABLE: &str = "wp_jnginx";
const SKIP_ENV: &str = "SKIP_DORIS_INTEGRATION_TESTS";

fn integration_spec() -> SinkSpec {
    let mut params = BTreeMap::new();
    params.insert("endpoint".into(), Value::String(TEST_DORIS_ENDPOINT.into()));
    params.insert("database".into(), Value::String(TEST_DORIS_DB.into()));
    params.insert("user".into(), Value::String(TEST_DORIS_USER.into()));
    params.insert("password".into(), Value::String(TEST_DORIS_PASSWORD.into()));
    params.insert("table".into(), Value::String(TEST_DORIS_TABLE.into()));
    params.insert("timeout_secs".into(), Value::from(30));
    params.insert("max_retries".into(), Value::from(3));
    SinkSpec {
        name: "doris_integration".into(),
        kind: "doris".into(),
        connector_id: String::new(),
        group: "integration".into(),
        params,
        filter: None,
    }
}

fn integration_config() -> DorisSinkConfig {
    DorisSinkConfig {
        endpoint: TEST_DORIS_ENDPOINT.into(),
        database: TEST_DORIS_DB.into(),
        user: TEST_DORIS_USER.into(),
        password: TEST_DORIS_PASSWORD.into(),
        table: TEST_DORIS_TABLE.into(),
        timeout_secs: 30,
        max_retries: 3,
        headers: None,
    }
}

#[test]
#[ignore = "requires running Doris instance"]
fn doris_sink_factory_validates_spec() -> anyhow::Result<()> {
    let spec = integration_spec();
    let factory = DorisSinkFactory;
    factory.validate_spec(&spec)?;
    Ok(())
}

#[test]
#[ignore = "requires running Doris instance"]
fn doris_sink_factory_rejects_invalid_endpoint() {
    let mut spec = integration_spec();
    spec.params
        .insert("endpoint".into(), Value::String("invalid-url".into()));
    let factory = DorisSinkFactory;
    let result = factory.validate_spec(&spec);
    assert!(result.is_err(), "should reject invalid endpoint");
}

#[tokio::test]
#[ignore = "requires running Doris instance"]
async fn doris_sink_connects_and_inserts_rows() -> anyhow::Result<()> {
    if should_skip_integration() {
        eprintln!("⚠️  Skipping Doris integration test ({} set)", SKIP_ENV);
        return Ok(());
    }

    let mut sink = DorisSink::new(integration_config()).await?;

    // 创建测试数据
    let mut record1 = DataRecord::default();
    record1.append(DataField::from_digit("wp_event_id", 1001));
    record1.append(DataField::from_chars("wp_src_key", "test"));
    record1.append(DataField::from_chars("date", "1707580800.0"));
    record1.append(DataField::from_chars("sip", "192.168.1.1"));
    record1.append(DataField::from_chars("timestamp", "2024-02-10 12:00:00"));
    record1.append(DataField::from_chars("http/request", "GET /test HTTP/1.1"));
    record1.append(DataField::from_digit("status", 200));
    record1.append(DataField::from_digit("size", 1024));
    record1.append(DataField::from_chars("referer", "https://example.com"));
    record1.append(DataField::from_chars("http/agent", "Mozilla/5.0"));

    let mut record2 = DataRecord::default();
    record2.append(DataField::from_digit("wp_event_id", 1002));
    record2.append(DataField::from_chars("wp_src_key", "test"));
    record2.append(DataField::from_chars("date", "1707580801.0"));
    record2.append(DataField::from_chars("sip", "192.168.1.2"));
    record2.append(DataField::from_chars("timestamp", "2024-02-10 12:00:01"));
    record2.append(DataField::from_chars("http/request", "POST /api HTTP/1.1"));
    record2.append(DataField::from_digit("status", 201));
    record2.append(DataField::from_digit("size", 2048));
    record2.append(DataField::from_chars("referer", "https://example.com"));
    record2.append(DataField::from_chars("http/agent", "curl/7.68.0"));

    // 批量插入
    sink.sink_record(&record1).await?;
    sink.sink_record(&record2).await?;
    sink.stop().await?;

    println!("✅ Successfully inserted 2 records into Doris");
    Ok(())
}

#[ignore = "requires running Doris instance"]
#[tokio::test]
async fn doris_sink_new_initializes_via_direct_config() -> anyhow::Result<()> {
    if should_skip_integration() {
        eprintln!("⚠️  Skipping Doris integration test ({} set)", SKIP_ENV);
        return Ok(());
    }

    let _sink = DorisSink::new(integration_config()).await?;
    Ok(())
}

fn should_skip_integration() -> bool {
    std::env::var(SKIP_ENV).is_ok()
}
