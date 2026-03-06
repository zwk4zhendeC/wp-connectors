//! HTTP Sink 并发性能测试程序
//!
//! 测试不同格式和压缩选项下的并发性能：
//! - 不压缩: json, ndjson, csv, kv
//! - gzip 压缩: json, ndjson, csv, kv
//!
//! 运行方式：
//! ```bash
//! cargo run --example http_sink_concurrent_test --features http --release
//! ```

// 引入共享的工具函数（从上级目录）
#[path = "../common_utils.rs"]
mod common_utils;

use std::sync::Arc;
use std::time::Instant;
use tokio::time::Duration;
use wp_connector_api::AsyncRecordSink;
use wp_connectors::http::{HttpSink, HttpSinkConfig};
use wp_model_core::model::DataRecord;

// ========================================
// 配置常量
// ========================================

/// HTTP 端点
const HTTP_ENDPOINT: &str = "http://localhost:8080/ingest";

/// HTTP 方法
const HTTP_METHOD: &str = "POST";

/// 请求超时时间（秒）
const TIMEOUT_SECS: u64 = 30;

/// 最大重试次数
const MAX_RETRIES: i32 = 3;

/// 连续失败阈值（超过此次数停止任务）
const MAX_CONSECUTIVE_ERRORS: usize = 3;

/// 失败后重试等待时间（毫秒）
const ERROR_RETRY_DELAY_MS: u64 = 100;

/// 进度打印间隔（每发送多少批次打印一次）
const PROGRESS_PRINT_INTERVAL: usize = 10;

// ========================================
// 测试场景定义
// ========================================

#[derive(Debug, Clone)]
struct TestScenario {
    name: String,
    format: String,
    compression: String,
    total_records: usize,
    task_count: usize,
    batch_size: usize,
}

impl TestScenario {
    fn new(
        name: &str,
        format: &str,
        compression: &str,
        total_records: usize,
        task_count: usize,
        batch_size: usize,
    ) -> Self {
        Self {
            name: name.to_string(),
            format: format.to_string(),
            compression: compression.to_string(),
            total_records,
            task_count,
            batch_size,
        }
    }
}

/// 获取所有测试场景
fn get_test_scenarios() -> Vec<TestScenario> {
    vec![
        // 不压缩场景 - 较大的批次和并发数
        TestScenario::new("JSON (无压缩)", "json", "none", 200_0000, 4, 1_0000),
        TestScenario::new("NDJSON (无压缩)", "ndjson", "none", 200_0000, 4, 1_0000),
        TestScenario::new("CSV (无压缩)", "csv", "none", 200_0000, 4, 1_0000),
        TestScenario::new("KV (无压缩)", "kv", "none", 200_0000, 4, 1_0000),
        // gzip 压缩场景 - 可以使用更大的批次（因为压缩后体积小）
        TestScenario::new("JSON (gzip)", "json", "gzip", 400_0000, 4, 10_0000),
        TestScenario::new("NDJSON (gzip)", "ndjson", "gzip", 400_0000, 4, 10_0000),
        TestScenario::new("CSV (gzip)", "csv", "gzip", 400_0000, 4, 10_0000),
        TestScenario::new("KV (gzip)", "kv", "gzip", 400_0000, 4, 10_0000),
    ]
}

// ========================================
// 创建 HTTP Sink
// ========================================

/// 创建测试用的 HttpSink
async fn create_test_sink(format: &str, compression: &str) -> HttpSink {
    let config = HttpSinkConfig::new(
        HTTP_ENDPOINT.to_string(),
        Some(HTTP_METHOD.to_string()),
        None, // username
        None, // password
        None, // headers
        Some(format.to_string()),
        None, // batch_size (使用默认值 1)
        Some(TIMEOUT_SECS),
        Some(MAX_RETRIES),
        Some(compression.to_string()),
    );

    HttpSink::new(config)
        .await
        .expect("Failed to create HttpSink")
}

// ========================================
// 并发发送任务
// ========================================

/// 单个并发任务：发送指定数量的记录
async fn send_task(
    task_id: usize,
    total_records: usize,
    batch_size: usize,
    format: String,
    compression: String,
) -> Result<(usize, Duration), String> {
    let mut sink = create_test_sink(&format, &compression).await;
    let start_time = Instant::now();

    let mut sent_count = 0;
    let mut batch_count = 0;
    let mut consecutive_errors = 0;

    while sent_count < total_records {
        // 计算本批次大小
        let current_batch_size = std::cmp::min(batch_size, total_records - sent_count);

        // 生成批量记录
        let records: Vec<Arc<DataRecord>> = (0..current_batch_size)
            .map(|i| Arc::new(common_utils::create_sample_record((sent_count + i) as i64)))
            .collect();

        // 发送批量记录
        match sink.sink_records(records).await {
            Ok(_) => {
                sent_count += current_batch_size;
                batch_count += 1;
                consecutive_errors = 0; // 重置连续失败计数

                // 定期打印进度
                if batch_count % PROGRESS_PRINT_INTERVAL == 0 {
                    let elapsed = start_time.elapsed();
                    let rate = sent_count as f64 / elapsed.as_secs_f64();
                    println!(
                        "  [Task {}] 已发送 {}/{} 条记录 ({:.0} records/s)",
                        task_id, sent_count, total_records, rate
                    );
                }
            }
            Err(e) => {
                consecutive_errors += 1;
                eprintln!(
                    "  [Task {}] 发送失败 (连续失败 {}/{}): {}",
                    task_id, consecutive_errors, MAX_CONSECUTIVE_ERRORS, e
                );

                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
                    return Err(format!(
                        "Task {} 连续失败 {} 次，停止任务",
                        task_id, consecutive_errors
                    ));
                }

                // 等待后重试
                tokio::time::sleep(Duration::from_millis(ERROR_RETRY_DELAY_MS)).await;
            }
        }
    }

    let elapsed = start_time.elapsed();
    Ok((sent_count, elapsed))
}

// ========================================
// 运行单个测试场景
// ========================================

/// 运行单个测试场景
async fn run_test_scenario(scenario: &TestScenario) -> Duration {
    println!("\n========================================");
    println!("测试场景: {}", scenario.name);
    println!("========================================");
    println!("  格式: {}", scenario.format);
    println!("  压缩: {}", scenario.compression);
    println!("  总记录数: {}", scenario.total_records);
    println!("  并发任务数: {}", scenario.task_count);
    println!("  批次大小: {}", scenario.batch_size);
    println!();

    let records_per_task = scenario.total_records / scenario.task_count;
    let start_time = Instant::now();

    // 启动并发任务
    let mut tasks = Vec::new();
    for task_id in 0..scenario.task_count {
        let format = scenario.format.clone();
        let compression = scenario.compression.clone();
        let batch_size = scenario.batch_size;

        let task = tokio::spawn(async move {
            send_task(task_id, records_per_task, batch_size, format, compression).await
        });

        tasks.push(task);
    }

    // 等待所有任务完成
    let mut total_sent = 0;
    let mut failed_tasks = 0;

    for (task_id, task) in tasks.into_iter().enumerate() {
        match task.await {
            Ok(Ok((sent, elapsed))) => {
                total_sent += sent;
                println!(
                    "  [Task {}] 完成: {} 条记录，耗时 {:.2}s",
                    task_id,
                    sent,
                    elapsed.as_secs_f64()
                );
            }
            Ok(Err(e)) => {
                failed_tasks += 1;
                eprintln!("  [Task {}] 失败: {}", task_id, e);
            }
            Err(e) => {
                failed_tasks += 1;
                eprintln!("  [Task {}] 任务崩溃: {}", task_id, e);
            }
        }
    }

    let total_elapsed = start_time.elapsed();

    // 打印统计信息
    println!("\n----------------------------------------");
    println!("测试结果:");
    println!("  成功发送: {} 条记录", total_sent);
    println!("  失败任务: {} 个", failed_tasks);

    if total_sent > 0 {
        let throughput = total_sent as f64 / total_elapsed.as_secs_f64();
        println!("  平均吞吐量: {:.0} EPS", throughput);
        println!(
            "  平均延迟: {:.2}ms/record",
            total_elapsed.as_millis() as f64 / total_sent as f64
        );
    }

    println!("----------------------------------------\n");

    // 测试间隔，避免服务器压力过大
    tokio::time::sleep(Duration::from_secs(2)).await;

    total_elapsed
}

// ========================================
// 主函数
// ========================================

#[tokio::main]
async fn main() {
    // 初始化日志（设置为 Error 级别，减少输出）
    let _ = env_logger::Builder::new()
        .filter_level(log::LevelFilter::Error)
        .try_init();

    println!("\n╔════════════════════════════════════════╗");
    println!("║   HTTP Sink 并发性能测试               ║");
    println!("╚════════════════════════════════════════╝\n");

    println!("全局配置:");
    println!("  HTTP 端点: {}", HTTP_ENDPOINT);
    println!("  超时时间: {}s", TIMEOUT_SECS);
    println!("  最大重试: {} 次", MAX_RETRIES);
    println!();

    // 获取所有测试场景
    let scenarios = get_test_scenarios();
    println!("将运行 {} 个测试场景", scenarios.len());
    println!("注意: 每个场景有独立的记录数、并发数和批次大小配置\n");

    // 运行所有测试场景
    let overall_start = Instant::now();
    let mut results = Vec::new();

    for (idx, scenario) in scenarios.iter().enumerate() {
        println!(
            "▶ 场景 {}/{}: {} ({}条记录, {}并发, {}批次)",
            idx + 1,
            scenarios.len(),
            scenario.name,
            scenario.total_records,
            scenario.task_count,
            scenario.batch_size
        );

        let scenario_elapsed = run_test_scenario(scenario).await;

        results.push((
            scenario.name.clone(),
            scenario_elapsed,
            scenario.total_records,
            scenario.task_count,
            scenario.batch_size,
        ));
    }

    let overall_elapsed = overall_start.elapsed();

    // 打印总结
    println!("\n╔════════════════════════════════════════════════════════════════════════════╗");
    println!("║                              测试总结                                      ║");
    println!("╚════════════════════════════════════════════════════════════════════════════╝\n");

    println!("各场景性能详情:");
    println!(
        "{:<25} {:>10} {:>6} {:>8} {:>12}",
        "场景", "总记录数", "并发", "批次", "EPS"
    );
    println!("{}", "-".repeat(66));

    for (name, elapsed, total_records, task_count, batch_size) in &results {
        let eps = *total_records as f64 / elapsed.as_secs_f64();
        println!(
            "{:<25} {:>10} {:>6} {:>8} {:>12.0}",
            name, total_records, task_count, batch_size, eps
        );
    }

    let total_records: usize = results.iter().map(|(_, _, count, _, _)| count).sum();
    let overall_throughput = total_records as f64 / overall_elapsed.as_secs_f64();

    println!("\n{}", "=".repeat(66));
    println!("总体统计:");
    println!("  总记录数: {}", total_records);
    println!("  总耗时: {:.2}s", overall_elapsed.as_secs_f64());
    println!("  平均吞吐量: {:.0} EPS", overall_throughput);

    println!("\n✅ 所有测试完成！\n");
}
