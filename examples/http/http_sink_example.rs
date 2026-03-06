//! HTTP Sink 示例程序
//!
//! 演示如何使用 HTTP Sink 发送数据到 HTTP/HTTPS 端点
//!
//! 运行方式：
//! ```bash
//! cargo run --example http_sink_example --features http
//! ```

// 引入共享的工具函数（从上级目录）
#[path = "../common_utils.rs"]
mod common_utils;

use std::collections::HashMap;
use std::sync::Arc;
use wp_connector_api::AsyncRecordSink;
use wp_connectors::http::{HttpSink, HttpSinkConfig};
use wp_model_core::model::DataRecord;

// ========================================
// 配置常量
// ========================================

/// HTTP 端点 URL
const HTTP_ENDPOINT: &str = "http://localhost:8081/ingest";

/// HTTP 方法
const HTTP_METHOD: &str = "POST";

/// 输出格式
const OUTPUT_FORMAT: &str = "json";

/// 批量大小
const BATCH_SIZE: usize = 2;

/// 请求超时时间（秒）
const TIMEOUT_SECS: u64 = 30;

/// 最大重试次数
const MAX_RETRIES: i32 = 3;

/// 压缩算法
const COMPRESSION: &str = "none";

/// 创建测试用的 HttpSink
async fn create_test_sink() -> HttpSink {
    // 创建自定义 HTTP 头
    let mut headers = HashMap::new();
    headers.insert("X-Custom-Header".to_string(), "test-value".to_string());
    headers.insert("X-API-Key".to_string(), "your-api-key-here".to_string());

    let config = HttpSinkConfig::new(
        HTTP_ENDPOINT.to_string(),
        Some(HTTP_METHOD.to_string()),
        None, // username
        None, // password
        Some(headers),
        Some(OUTPUT_FORMAT.to_string()),
        Some(BATCH_SIZE),
        Some(TIMEOUT_SECS),
        Some(MAX_RETRIES),
        Some(COMPRESSION.to_string()),
    );

    HttpSink::new(config)
        .await
        .expect("Failed to create HttpSink")
}

/// 示例 1: 发送单条记录
async fn example_single_record() {
    println!("\n========================================");
    println!("示例 1: 发送单条记录");
    println!("========================================\n");

    let mut sink = create_test_sink().await;

    // 创建一条测试记录
    let record = common_utils::create_sample_record(1);

    println!("📤 发送单条记录到: {}", HTTP_ENDPOINT);
    println!("   格式: {}", OUTPUT_FORMAT);
    println!("   方法: {}", HTTP_METHOD);

    // 发送记录
    match sink.sink_record(&record).await {
        Ok(_) => {
            println!("✅ 记录发送成功");
        }
        Err(e) => {
            eprintln!("❌ 记录发送失败: {}", e);
        }
    }
}

/// 示例 2: 批量发送记录
async fn example_batch_records() {
    println!("\n========================================");
    println!("示例 2: 批量发送记录");
    println!("========================================\n");

    let mut sink = create_test_sink().await;

    // 创建一批测试记录
    let records: Vec<Arc<DataRecord>> = (1..=BATCH_SIZE)
        .map(|i| Arc::new(common_utils::create_sample_record(i as i64)))
        .collect();

    println!("📤 批量发送 {} 条记录到: {}", records.len(), HTTP_ENDPOINT);
    println!("   格式: {}", OUTPUT_FORMAT);
    println!("   方法: {}", HTTP_METHOD);

    // 批量发送记录
    match sink.sink_records(records).await {
        Ok(_) => {
            println!("✅ 批量记录发送成功");
        }
        Err(e) => {
            eprintln!("❌ 批量记录发送失败: {}", e);
        }
    }
}

/// 示例 3: 使用 Basic 认证
async fn example_with_auth() {
    println!("\n========================================");
    println!("示例 3: 使用 HTTP Basic 认证");
    println!("========================================\n");

    let config = HttpSinkConfig::new(
        HTTP_ENDPOINT.to_string(),
        Some(HTTP_METHOD.to_string()),
        Some("username".to_string()), // Basic Auth 用户名
        Some("password".to_string()), // Basic Auth 密码
        None,                         // headers
        Some(OUTPUT_FORMAT.to_string()),
        Some(1),
        Some(TIMEOUT_SECS),
        Some(MAX_RETRIES),
        Some(COMPRESSION.to_string()),
    );

    let mut sink = HttpSink::new(config)
        .await
        .expect("Failed to create HttpSink with auth");

    let record = common_utils::create_sample_record(100);

    println!("📤 发送带认证的记录到: {}", HTTP_ENDPOINT);
    println!("   用户名: username");
    println!("   密码: ********");

    match sink.sink_record(&record).await {
        Ok(_) => {
            println!("✅ 带认证的记录发送成功");
        }
        Err(e) => {
            eprintln!("❌ 带认证的记录发送失败: {}", e);
        }
    }
}

/// 示例 4: 使用 Gzip 压缩
async fn example_with_compression() {
    println!("\n========================================");
    println!("示例 4: 使用 Gzip 压缩");
    println!("========================================\n");

    let config = HttpSinkConfig::new(
        HTTP_ENDPOINT.to_string(),
        Some(HTTP_METHOD.to_string()),
        None,
        None,
        None,
        Some(OUTPUT_FORMAT.to_string()),
        Some(BATCH_SIZE),
        Some(TIMEOUT_SECS),
        Some(MAX_RETRIES),
        Some("gzip".to_string()), // 启用 gzip 压缩
    );

    let mut sink = HttpSink::new(config)
        .await
        .expect("Failed to create HttpSink with compression");

    let records: Vec<Arc<DataRecord>> = (1..=BATCH_SIZE)
        .map(|i| Arc::new(common_utils::create_sample_record(i as i64)))
        .collect();

    println!("📤 发送压缩数据到: {}", HTTP_ENDPOINT);
    println!("   压缩算法: gzip");
    println!("   记录数: {}", records.len());

    match sink.sink_records(records).await {
        Ok(_) => {
            println!("✅ 压缩数据发送成功");
        }
        Err(e) => {
            eprintln!("❌ 压缩数据发送失败: {}", e);
        }
    }
}

/// 示例 5: 不同的输出格式
async fn example_different_formats() {
    println!("\n========================================");
    println!("示例 5: 不同的输出格式");
    println!("========================================\n");

    // let formats = vec!["json", "ndjson", "csv", "kv", "raw", "proto-text"];
    let formats = vec!["json", "ndjson", "csv", "kv", "raw", "proto-text"];
    for fmt in formats {
        println!("\n📋 测试格式: {}", fmt);
        // ;
        let config = HttpSinkConfig::new(
            // HTTP_ENDPOINT.to_string()+"/",
            format!("{}/{}",HTTP_ENDPOINT.to_string(),fmt.to_string()),
            Some(HTTP_METHOD.to_string()),
            None,
            None,
            None,
            Some(fmt.to_string()),
            Some(1),
            Some(TIMEOUT_SECS),
            Some(MAX_RETRIES),
            Some(COMPRESSION.to_string()),
        );

        let mut sink = match HttpSink::new(config).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("   ❌ 创建 Sink 失败: {}", e);
                continue;
            }
        };

        let record = common_utils::create_sample_record(200);

        match sink.sink_record(&record).await {
            Ok(_) => {
                println!("   ✅ {} 格式发送成功", fmt);
            }
            Err(e) => {
                eprintln!("   ❌ {} 格式发送失败: {}", fmt, e);
            }
        }
    }
}

/// 示例 6: 错误处理和重试
async fn example_error_handling() {
    println!("\n========================================");
    println!("示例 6: 错误处理和重试");
    println!("========================================\n");

    // 使用一个不存在的端点来演示错误处理
    let invalid_endpoint = "http://localhost:9999/nonexistent";

    let config = HttpSinkConfig::new(
        invalid_endpoint.to_string(),
        Some(HTTP_METHOD.to_string()),
        None,
        None,
        None,
        Some(OUTPUT_FORMAT.to_string()),
        Some(1),
        Some(5), // 短超时
        Some(2), // 只重试 2 次
        Some(COMPRESSION.to_string()),
    );

    let mut sink = HttpSink::new(config)
        .await
        .expect("Failed to create HttpSink");

    let record = common_utils::create_sample_record(300);

    println!("📤 尝试发送到无效端点: {}", invalid_endpoint);
    println!("   最大重试次数: 2");
    println!("   超时时间: 5 秒");

    match sink.sink_record(&record).await {
        Ok(_) => {
            println!("✅ 记录发送成功（不太可能）");
        }
        Err(e) => {
            println!("❌ 记录发送失败（预期行为）");
            println!("   错误信息: {}", e);
            println!("   💡 这演示了 HTTP Sink 的错误处理和重试机制");
        }
    }
}

#[tokio::main]
async fn main() {
    // 初始化日志
    let _ = env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .try_init();

    println!("\n╔════════════════════════════════════════╗");
    println!("║     HTTP Sink 示例程序                 ║");
    println!("╚════════════════════════════════════════╝");

    println!("\n💡 提示:");
    println!("   本示例需要一个 HTTP 服务器监听 {}", HTTP_ENDPOINT);
    println!("   你可以使用以下命令启动一个简单的测试服务器:");
    println!("   python3 -m http.server 8080");
    println!("   或使用 webhook.site 等在线服务");
    println!();

    // 运行所有示例
    example_single_record().await;
    example_batch_records().await;
    example_with_auth().await;
    example_with_compression().await;
    example_different_formats().await;
    example_error_handling().await;

    println!("\n========================================");
    println!("所有示例执行完成");
    println!("========================================\n");

    println!("💡 配置说明:");
    println!("   - endpoint: HTTP(S) 端点 URL（必填）");
    println!("   - method: HTTP 方法（默认 POST）");
    println!("   - username/password: Basic 认证（可选）");
    println!("   - headers: 自定义 HTTP 头（可选）");
    println!("   - fmt: 输出格式（默认 json）");
    println!("   - batch_size: 批量大小（默认 1）");
    println!("   - timeout_secs: 超时时间（默认 60）");
    println!("   - max_retries: 最大重试次数（默认 3）");
    println!("   - compression: 压缩算法（默认 none）");
    println!();

    println!("💡 支持的输出格式:");
    println!("   - json: JSON 对象");
    println!("   - ndjson: 换行分隔的 JSON");
    println!("   - csv: CSV 格式（带表头）");
    println!("   - kv: 键值对格式");
    println!("   - raw: 原始字段值");
    println!("   - proto-text: Protocol Buffer 文本格式");
    println!();

    println!("💡 支持的压缩算法:");
    println!("   - none: 不压缩（默认）");
    println!("   - gzip: Gzip 压缩");
    println!();
}
