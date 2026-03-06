//! ClickHouse 并发异步发送测试程序
//!
//! 用于测试多个异步任务并发发送数据到 ClickHouse
//!
//! 运行方式：
//! ```bash
//! cargo run --example clickhouse_sink_concurrent_test --features clickhouse
//! ```

// 引入共享的工具函数
#[path = "common_utils.rs"]
mod common_utils;

use std::sync::Arc;
use std::time::Instant;
use wp_connector_api::AsyncRecordSink;
use wp_connectors::clickhouse::{ClickHouseSink, ClickHouseSinkConfig};
use wp_model_core::model::DataRecord;

// ========================================
// 配置常量
// ========================================

/// ClickHouse 主机地址
const CH_HOST: &str = "localhost";

/// ClickHouse 端口号（HTTP 接口）
const CH_PORT: u16 = 8123;

/// ClickHouse 数据库名称
const CH_DATABASE: &str = "default";

/// ClickHouse 表名称
const CH_TABLE: &str = "wp_nginx";

/// ClickHouse 用户名
const CH_USERNAME: &str = "default";

/// ClickHouse 密码
const CH_PASSWORD: &str = "default";

/// 请求超时时间（秒）
const CH_TIMEOUT_SECS: u64 = 30;

/// 最大重试次数
const CH_MAX_RETRIES: i32 = 3;

/// 总记录数
const TOTAL_RECORDS: usize = 2; // 200w

/// 并发任务数
const TASK_COUNT: usize = 1;

/// 每批次大小
const BATCH_SIZE: usize = 2; // 10w

/// 连续失败阈值（超过此次数停止任务）
const MAX_CONSECUTIVE_ERRORS: usize = 3;

/// 失败后重试等待时间（毫秒）
const ERROR_RETRY_DELAY_MS: u64 = 100;

/// 进度打印间隔（每发送多少批次打印一次）
const PROGRESS_PRINT_INTERVAL: usize = 10;

/// 创建测试用的 ClickHouseSink
async fn create_test_sink() -> ClickHouseSink {
    let config = ClickHouseSinkConfig::new(
        CH_HOST.to_string(),
        Some(CH_PORT),
        CH_DATABASE.to_string(),
        CH_TABLE.to_string(),
        CH_USERNAME.to_string(),
        CH_PASSWORD.to_string(),
        Some(CH_TIMEOUT_SECS),
        Some(CH_MAX_RETRIES),
    );

    ClickHouseSink::new(config)
        .await
        .expect("Failed to create ClickHouseSink")
}

#[tokio::main]
async fn main() {
    // 初始化日志（硬编码为 Info 级别，忽略环境变量）
    let _ = env_logger::Builder::new()
        .filter_level(log::LevelFilter::Info)
        .try_init();

    println!("\n========================================");
    println!("ClickHouse 并发异步发送测试");
    println!("========================================\n");

    let records_per_task = TOTAL_RECORDS / TASK_COUNT;

    println!("测试配置:");
    println!("  ClickHouse 端点: {}:{}", CH_HOST, CH_PORT);
    println!("  数据库: {}", CH_DATABASE);
    println!("  表名称: {}", CH_TABLE);
    println!("  总记录数: {}", TOTAL_RECORDS);
    println!("  并发任务数: {}", TASK_COUNT);
    println!("  每批次大小: {}", BATCH_SIZE);
    println!("  每任务记录数: {}", records_per_task);
    println!();

    // 预先创建所有 sink 实例
    println!("📦 创建 {} 个 ClickHouseSink 实例...", TASK_COUNT);
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
        println!("💡 验证数据是否到达 ClickHouse:");
        println!(
            "   SELECT COUNT(*) FROM {}.{} WHERE wp_src_key LIKE 'concurrent_test_%';",
            CH_DATABASE, CH_TABLE
        );
        println!("   -- 应该返回 {}", total_sent);
        println!();
        println!("💡 查看数据样例:");
        println!(
            "   SELECT * FROM {}.{} WHERE wp_src_key LIKE 'concurrent_test_%' LIMIT 5;",
            CH_DATABASE, CH_TABLE
        );
        println!();
        println!("💡 查看数据分布:");
        println!(
            "   SELECT wp_src_key, COUNT(*) as cnt FROM {}.{}",
            CH_DATABASE, CH_TABLE
        );
        println!("   WHERE wp_src_key LIKE 'concurrent_test_%'");
        println!("   GROUP BY wp_src_key ORDER BY cnt DESC LIMIT 10;");
    }

    if failed_tasks > 0 {
        println!("\n⚠️  有 {} 个任务未完成，请检查:", failed_tasks);
        println!("   1. ClickHouse 服务状态");
        println!("   2. ClickHouse 日志");
        println!("   3. 网络连接");
        println!("   4. 表结构是否匹配");
        println!("   5. 数据库和表是否存在");
    }

    println!("\n========================================\n");
}
