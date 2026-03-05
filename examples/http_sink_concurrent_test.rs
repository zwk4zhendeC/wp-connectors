//! HTTP Sink 并发发送测试程序
//!
//! 运行方式：
//! ```bash
//! cargo run --example http_sink_concurrent_test --features http
//! ```

#[path = "common_utils.rs"]
mod common_utils;

// use clickhouse::Client
#[cfg(feature = "http")]
use std::sync::Arc;
#[cfg(feature = "http")]
use std::time::Instant;
#[cfg(feature = "http")]
use wp_connector_api::AsyncRecordSink;
#[cfg(feature = "http")]
use wp_connectors::http::{HTTPSink, HTTPSinkConfig};
#[cfg(feature = "http")]
use wp_model_core::model::{DataRecord, fmt_def::TextFmt};

#[cfg(feature = "http")]
fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<T>().ok())
        .unwrap_or(default)
}

#[cfg(feature = "http")]
fn env_or_string(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[cfg(feature = "http")]
fn parse_fmt(raw: &str) -> TextFmt {
    let trimmed = raw.trim();
    if matches!(
        trimmed,
        "json" | "csv" | "show" | "kv" | "raw" | "proto" | "proto-text"
    ) {
        TextFmt::from(trimmed)
    } else {
        TextFmt::Json
    }
}

#[cfg(feature = "http")]
async fn create_test_sink(
    endpoint: &str,
    method: &str,
    fmt: TextFmt,
    content_type: &str,
    timeout_secs: u64,
    max_retries: i32,
) -> HTTPSink {
    let config = HTTPSinkConfig::new(
        endpoint.to_string(),
        Some(method.to_string()),
        Some(fmt),
        Some(content_type.to_string()),
        None,
        None,
        Some(timeout_secs),
        Some(max_retries),
        None,
    );

    HTTPSink::new(config)
        .await
        .expect("Failed to create HTTPSink")
}

#[cfg(feature = "http")]
#[tokio::main]
async fn main() {
    let _ = env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .try_init();

    let endpoint = env_or_string("HTTP_ENDPOINT", "http://localhost:8080/ingest");
    let method = env_or_string("HTTP_METHOD", "POST");
    let fmt = parse_fmt(&env_or_string("HTTP_FMT", "json"));
    let content_type = env_or_string("HTTP_CONTENT_TYPE", "application/x-ndjson");
    let timeout_secs = env_or("HTTP_TIMEOUT_SECS", 60_u64);
    let max_retries = env_or("HTTP_MAX_RETRIES", 3_i32);
    let total_records = env_or("HTTP_TOTAL_RECORDS", 100_000_usize);
    let task_count = env_or("HTTP_TASKS", 4_usize);
    let batch_size = env_or("HTTP_BATCH_SIZE", 2048_usize);

    println!("\n========================================");
    println!("HTTP Sink 并发异步发送测试");
    println!("========================================\n");

    println!("测试配置:");
    println!("  Endpoint: {}", endpoint);
    println!("  Method: {}", method);
    println!("  Fmt: {}", fmt);
    println!("  Content-Type: {}", content_type);
    println!("  Total records: {}", total_records);
    println!("  Task count: {}", task_count);
    println!("  Batch size: {}", batch_size);

    let records_per_task = total_records / task_count;

    let mut sinks = Vec::with_capacity(task_count);
    for _ in 0..task_count {
        sinks.push(
            create_test_sink(
                &endpoint,
                &method,
                fmt,
                &content_type,
                timeout_secs,
                max_retries,
            )
            .await,
        );
    }

    let mut handles = Vec::with_capacity(task_count);
    let start = Instant::now();

    for (task_id, mut sink) in sinks.into_iter().enumerate() {
        let handle = tokio::spawn(async move {
            let mut sent = 0_usize;
            let mut batches = 0_usize;

            while sent < records_per_task {
                let current_batch = batch_size.min(records_per_task - sent);
                let records: Vec<Arc<DataRecord>> = (0..current_batch)
                    .map(|i| {
                        Arc::new(common_utils::create_sample_record(
                            (task_id * records_per_task + sent + i) as i64,
                        ))
                    })
                    .collect();

                if sink.sink_records(records).await.is_err() {
                    break;
                }

                sent += current_batch;
                batches += 1;
            }

            (sent, batches)
        });
        handles.push(handle);
    }

    let mut total_sent = 0_usize;
    let mut total_batches = 0_usize;

    for handle in handles {
        if let Ok((sent, batches)) = handle.await {
            total_sent += sent;
            total_batches += batches;
        }
    }

    let elapsed = start.elapsed();
    println!("\n========================================");
    println!("测试结果");
    println!("========================================");
    println!("总发送记录: {}", total_sent);
    println!("总批次数: {}", total_batches);
    println!("总耗时: {:.2}s", elapsed.as_secs_f64());
    if elapsed.as_secs_f64() > 0.0 {
        println!(
            "吞吐量: {:.0} records/s",
            total_sent as f64 / elapsed.as_secs_f64()
        );
    }
    println!("========================================\n");
}

#[cfg(not(feature = "http"))]
fn main() {
    eprintln!("Enable with: cargo run --example http_sink_concurrent_test --features http");
}
