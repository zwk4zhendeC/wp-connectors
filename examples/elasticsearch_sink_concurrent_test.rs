//! Elasticsearch 并发异步发送测试程序
//!
//! 用于测试多个异步任务并发发送数据到 Elasticsearch
//!
//! 运行方式：
//! ```bash
//! cargo run --example elasticsearch_sink_concurrent_test --features elasticsearch
//! ```

// 引入共享的工具函数
#[path = "common_utils.rs"]
mod common_utils;

use std::sync::Arc;
use std::time::Instant;
use wp_connector_api::AsyncRecordSink;
use wp_connectors::elasticsearch::{ElasticsearchSink, ElasticsearchSinkConfig};
use wp_model_core::model::DataRecord;

// ========================================
// 配置常量
// ========================================

/// Elasticsearch 协议
const ES_PROTOCOL: &str = "http";

/// Elasticsearch 主机地址
const ES_HOST: &str = "localhost";

/// Elasticsearch 端口号
const ES_PORT: u16 = 9200;

/// Elasticsearch 索引名称
const ES_INDEX: &str = "wp_nginx";

/// Elasticsearch 用户名
const ES_USERNAME: &str = "elastic";

/// Elasticsearch 密码
const ES_PASSWORD: &str = "zgVClXP2";

/// 请求超时时间（秒）
const ES_TIMEOUT_SECS: u64 = 30;

/// 最大重试次数
const ES_MAX_RETRIES: i32 = 3;

/// 总记录数
const TOTAL_RECORDS: usize = 200_0000; // 200w

/// 并发任务数
const TASK_COUNT: usize = 4;

/// 每批次大小
const BATCH_SIZE: usize = 10_0000; // 10w

/// 连续失败阈值（超过此次数停止任务）
const MAX_CONSECUTIVE_ERRORS: usize = 3;

/// 失败后重试等待时间（毫秒）
const ERROR_RETRY_DELAY_MS: u64 = 100;

/// 进度打印间隔（每发送多少批次打印一次）
const PROGRESS_PRINT_INTERVAL: usize = 10;

/// 创建测试用的 ElasticsearchSink
async fn create_test_sink() -> ElasticsearchSink {
    let config = ElasticsearchSinkConfig::new(
        Some(ES_PROTOCOL.to_string()),
        ES_HOST.to_string(),
        Some(ES_PORT),
        ES_INDEX.to_string(),
        ES_USERNAME.to_string(),
        ES_PASSWORD.to_string(),
        Some(ES_TIMEOUT_SECS),
        Some(ES_MAX_RETRIES),
    );

    ElasticsearchSink::new(config)
        .await
        .expect("Failed to create ElasticsearchSink")
}

#[tokio::main]
async fn main() {
    // 初始化日志（硬编码为 Info 级别，忽略环境变量）
    let _ = env_logger::Builder::new()
        .filter_level(log::LevelFilter::Debug)
        .try_init();

    println!("\n========================================");
    println!("\n========================================");
    println!("Elasticsearch 并发异步发送测试");
    println!("========================================\n");

    let records_per_task = TOTAL_RECORDS / TASK_COUNT;

    println!("测试配置:");
    println!(
        "  Elasticsearch 端点: {}://{}:{}",
        ES_PROTOCOL, ES_HOST, ES_PORT
    );
    println!("  索引名称: {}", ES_INDEX);
    println!("  总记录数: {}", TOTAL_RECORDS);
    println!("  并发任务数: {}", TASK_COUNT);
    println!("  每批次大小: {}", BATCH_SIZE);
    println!("  每任务记录数: {}", records_per_task);
    println!();
    // 预先创建所有 sink 实例
    println!("📦 创建 {} 个 ElasticsearchSink 实例...", TASK_COUNT);
    let mut sinks = Vec::new();
    for i in 0..TASK_COUNT {
        let sink = create_test_sink().await;
        sinks.push(sink);
        if (i + 1) % 5 == 0 || i == TASK_COUNT - 1 {
            println!("  已创建: {}/{}", i + 1, TASK_COUNT);
        }
    }
    println!("✅ 所有 Sink 创建完成\n");

    let mut handles = vec![];

    // 创建并发任务（在计时外部）
    println!("📋 创建 {} 个异步任务...", TASK_COUNT);
    for (task_id, mut sink) in sinks.into_iter().enumerate() {
        let handle = tokio::spawn(async move {
            let mut sent = 0;
            let mut batch_count = 0;
            let mut error_count = 0;

            while sent < records_per_task {
                let current_batch = BATCH_SIZE.min(records_per_task - sent);

                // 创建一批数据
                let records: Vec<Arc<DataRecord>> = (0..current_batch)
                    .map(|i| {
                        Arc::new(common_utils::create_sample_record(
                            (task_id * records_per_task + sent + i) as i64,
                        ))
                    })
                    .collect();

                // 发送数据
                match sink.sink_records(records).await {
                    Ok(_) => {
                        sent += current_batch;
                        batch_count += 1;
                        error_count = 0; // 重置错误计数

                        // 每发送指定批次打印一次进度
                        if batch_count % PROGRESS_PRINT_INTERVAL == 0 {
                            println!(
                                "  任务 {}: 已发送 {}/{} ({:.1}%)",
                                task_id,
                                sent,
                                records_per_task,
                                (sent as f64 / records_per_task as f64) * 100.0
                            );
                        }
                    }
                    Err(e) => {
                        error_count += 1;
                        eprintln!(
                            "❌ 任务 {} 批次 {} 发送失败: {}",
                            task_id,
                            batch_count + 1,
                            e
                        );

                        // 连续失败达到阈值则停止该任务
                        if error_count >= MAX_CONSECUTIVE_ERRORS {
                            eprintln!("❌ 任务 {} 连续失败 {} 次，停止", task_id, error_count);
                            break;
                        }

                        // 失败后等待一下再继续
                        tokio::time::sleep(tokio::time::Duration::from_millis(
                            ERROR_RETRY_DELAY_MS,
                        ))
                        .await;
                    }
                }
            }

            (task_id, sent, batch_count)
        });
        handles.push(handle);
    }
    println!("✅ 所有异步任务创建完成\n");

    // 开始计时（在任务创建之后）
    println!("🚀 开始并发发送数据...\n");
    let start = Instant::now();

    // 等待所有任务完成
    println!("\n⏳ 等待所有任务完成...\n");
    let mut total_sent = 0;
    let mut total_batches = 0;
    let mut failed_tasks = 0;

    for handle in handles {
        match handle.await {
            Ok((task_id, sent, batches)) => {
                println!(
                    "✅ 任务 {} 完成: 发送 {} 条记录，{} 批次",
                    task_id, sent, batches
                );
                total_sent += sent;
                total_batches += batches;

                if sent < records_per_task {
                    failed_tasks += 1;
                }
            }
            Err(e) => {
                eprintln!("❌ 任务执行失败: {}", e);
                failed_tasks += 1;
            }
        }
    }

    let duration = start.elapsed();

    // 输出结果
    println!("\n========================================");
    println!("测试结果:");
    println!("========================================");
    println!("并发任务数: {}", TASK_COUNT);
    println!("成功任务数: {}", TASK_COUNT - failed_tasks);
    println!("失败任务数: {}", failed_tasks);
    println!("总记录数: {}", total_sent);
    println!("总批次数: {}", total_batches);
    println!("总耗时: {:.2}秒", duration.as_secs_f64());

    if total_sent > 0 {
        println!(
            "吞吐量: {:.0} 条/秒",
            total_sent as f64 / duration.as_secs_f64()
        );
        println!(
            "EPS (Events Per Second): {:.0}",
            total_sent as f64 / duration.as_secs_f64()
        );
        println!(
            "平均延迟: {:.2}ms/批次",
            duration.as_millis() as f64 / total_batches as f64
        );
        println!(
            "每任务平均 EPS: {:.0}",
            (total_sent as f64 / TASK_COUNT as f64) / duration.as_secs_f64()
        );
    }

    println!("========================================\n");

    // 验证建议
    if total_sent > 0 {
        let endpoint = format!("{}://{}:{}", ES_PROTOCOL, ES_HOST, ES_PORT);
        println!("💡 验证数据是否到达 Elasticsearch:");
        println!(
            "   curl -u {}:{} -s \"{}/{}/_count?pretty\"",
            ES_USERNAME, ES_PASSWORD, endpoint, ES_INDEX
        );
        println!("   -- 应该返回 count: {}", total_sent);
        println!();
        println!("💡 查看数据样例:");
        println!(
            "   curl -u {}:{} -s \"{}/{}/_search?pretty&size=5\"",
            ES_USERNAME, ES_PASSWORD, endpoint, ES_INDEX
        );
    }

    if failed_tasks > 0 {
        println!("\n⚠️  有 {} 个任务未完成，请检查:", failed_tasks);
        println!("   1. Elasticsearch 服务状态");
        println!("   2. Elasticsearch 日志");
        println!("   3. 网络连接");
        println!("   4. 索引是否存在");
    }

    println!("\n========================================\n");
}
