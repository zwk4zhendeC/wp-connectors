//! Doris 多异步任务发送测试程序
//!
//! 类似于 test_single_async_send_1m_records，但使用多个异步任务并发发送
//! 每个任务有独立的 Sink 实例，显示进度和性能统计
//!
//! 运行方式：
//! ```bash
//! cargo run --example doris_multi_async_test --features doris
//! ```

use std::sync::Arc;
use std::time::Instant;
use wp_connector_api::AsyncRecordSink;
use wp_connectors::doris::{DorisSink, DorisSinkConfig};
use wp_model_core::model::{DataField, DataRecord};

/// 创建测试用的 DataRecord（与 wp_nginx 表结构完全匹配）
fn create_sample_record(id: i64) -> DataRecord {
    let mut record = DataRecord::default();
    record.append(DataField::from_digit("wp_event_id", id));
    record.append(DataField::from_chars(
        "wp_src_key",
        format!("multi_async_test_{}", id),
    ));
    record.append(DataField::from_chars("sip", "192.168.1.100"));
    record.append(DataField::from_chars("timestamp", "2024-03-02 10:00:00"));
    record.append(DataField::from_chars(
        "http/request",
        format!("GET /api/test/{} HTTP/1.1", id),
    ));
    record.append(DataField::from_digit("status", 200));
    record.append(DataField::from_digit("size", 1024));
    record.append(DataField::from_chars("referer", "https://example.com/test"));
    record.append(DataField::from_chars(
        "http/agent",
        "Mozilla/5.0 (Multi Async Test)",
    ));
    record
}

/// 创建测试用的 DorisSink
async fn create_test_sink() -> DorisSink {
    let mut headers = std::collections::HashMap::new();
    headers.insert("max_filter_ratio".to_string(), "0".to_string());

    let config = DorisSinkConfig::new(
        "http://localhost:8040".to_string(),
        "test_db".to_string(),
        "wp_nginx".to_string(),
        "root".to_string(),
        "".to_string(),
        Some(30),
        Some(3),
        Some(headers),
    );

    DorisSink::new(config)
        .await
        .expect("Failed to create DorisSink")
}

#[tokio::main]
async fn main() {
    // 初始化日志（硬编码为 Error 级别，忽略环境变量）
    let _ = env_logger::Builder::new()
        .filter_level(log::LevelFilter::Error)
        .try_init();

    println!("\n========================================");
    println!("Doris 多异步任务发送测试");
    println!("========================================\n");

    // 配置参数（可根据需要调整）
    let total_records = 100_000; // 总记录数：10万
    let task_count = 10; // 并发任务数：10个
    let batch_size = 1_000; // 每批次大小：1千条
    let records_per_task = total_records / task_count;

    println!("测试配置:");
    println!("  总记录数: {}", total_records);
    println!("  并发任务数: {}", task_count);
    println!("  每批次大小: {}", batch_size);
    println!("  每任务记录数: {}", records_per_task);
    println!();

    // 预先创建所有 sink 实例
    println!("📦 创建 {} 个 DorisSink 实例...", task_count);
    let mut sinks = Vec::new();
    for i in 0..task_count {
        let sink = create_test_sink().await;
        sinks.push(sink);
        if (i + 1) % 5 == 0 || i == task_count - 1 {
            println!("  已创建: {}/{}", i + 1, task_count);
        }
    }
    println!("✅ 所有 Sink 创建完成\n");

    let mut handles = vec![];

    // 创建并发任务（在计时外部）
    println!("📋 创建 {} 个异步任务...", task_count);
    for (task_id, mut sink) in sinks.into_iter().enumerate() {
        let handle = tokio::spawn(async move {
            let mut sent = 0;
            let mut batch_count = 0;
            let mut error_count = 0;
            let mut success_count = 0;

            while sent < records_per_task {
                let current_batch = batch_size.min(records_per_task - sent);

                // 创建一批数据
                let records: Vec<Arc<DataRecord>> = (0..current_batch)
                    .map(|i| {
                        Arc::new(create_sample_record(
                            (task_id * records_per_task + sent + i) as i64,
                        ))
                    })
                    .collect();

                // 发送数据
                match sink.sink_records(records).await {
                    Ok(_) => {
                        sent += current_batch;
                        batch_count += 1;
                        success_count += 1;
                        error_count = 0; // 重置错误计数

                        // 每发送10批打印一次进度
                        if batch_count % 10 == 0 {
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

                        // 只在前几次错误时打印详细信息
                        if error_count <= 3 {
                            eprintln!("   批次大小: {}", current_batch);
                            eprintln!("   已发送: {}/{}", sent, records_per_task);
                            eprintln!("   成功批次: {}", success_count);
                        }

                        // 连续失败3次则停止该任务
                        if error_count >= 3 {
                            eprintln!("❌ 任务 {} 连续失败 {} 次，停止", task_id, error_count);
                            break;
                        }

                        // 失败后等待一下再继续
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    }
                }
            }

            (task_id, sent, batch_count, success_count, error_count)
        });

        handles.push(handle);
    }
    println!("✅ 所有异步任务创建完成\n");

    // 开始计时（在任务创建之后）
    println!("🚀 开始并发发送数据...\n");
    let start = Instant::now();

    // 等待所有任务完成
    println!("⏳ 等待所有任务完成...\n");
    let mut total_sent = 0;
    let mut total_batches = 0;
    let mut total_success = 0;
    let mut failed_tasks = 0;

    for handle in handles {
        match handle.await {
            Ok((task_id, sent, batches, success, errors)) => {
                let status = if sent >= records_per_task {
                    "✅"
                } else {
                    "⚠️"
                };
                println!(
                    "{} 任务 {} 完成: 发送 {} 条记录，{} 批次 (成功: {}, 失败: {})",
                    status, task_id, sent, batches, success, errors
                );
                total_sent += sent;
                total_batches += batches;
                total_success += success;

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

    // 输出最终结果
    println!("\n========================================");
    println!("测试结果:");
    println!("========================================");
    println!("并发任务数: {}", task_count);
    println!("成功任务数: {}", task_count - failed_tasks);
    println!("失败任务数: {}", failed_tasks);
    println!("总记录数: {}", total_sent);
    println!("总批次数: {}", total_batches);
    println!("成功批次数: {}", total_success);
    println!("总耗时: {:.2}秒", duration.as_secs_f64());

    if total_sent > 0 {
        println!(
            "平均吞吐量: {:.0} 条/秒",
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
            (total_sent as f64 / task_count as f64) / duration.as_secs_f64()
        );
        println!(
            "每批次平均记录数: {:.0}",
            total_sent as f64 / total_batches as f64
        );
    }

    println!("========================================\n");

    // 验证建议
    if total_sent > 0 {
        println!("💡 验证数据是否到达 Doris:");
        println!(
            "   SELECT COUNT(*) FROM test_db.wp_nginx WHERE wp_src_key LIKE 'multi_async_test_%';"
        );
        println!("   -- 应该返回 {}", total_sent);
        println!();
        println!("💡 查看数据分布:");
        println!("   SELECT wp_src_key, COUNT(*) as cnt FROM test_db.wp_nginx");
        println!("   WHERE wp_src_key LIKE 'multi_async_test_%'");
        println!("   GROUP BY wp_src_key ORDER BY cnt DESC LIMIT 10;");
        println!();
        println!("💡 查看数据范围:");
        println!("   SELECT MIN(wp_event_id), MAX(wp_event_id) FROM test_db.wp_nginx");
        println!("   WHERE wp_src_key LIKE 'multi_async_test_%';");
    }

    if failed_tasks > 0 {
        println!("\n⚠️  有 {} 个任务未完成，请检查:", failed_tasks);
        println!("   1. Doris 服务状态");
        println!("   2. Doris BE 日志");
        println!("   3. 网络连接");
        println!("   4. 表结构是否匹配");
        println!("   5. 资源限制（内存、连接数等）");
    }

    println!("\n========================================\n");
}
