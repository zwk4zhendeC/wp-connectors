#![cfg(feature = "doris")]
//! Doris 性能测试 - 使用 tokio::test 和手动计时

use std::sync::Arc;
use std::time::Instant;
use wp_connector_api::AsyncRecordSink;
use wp_connectors::doris::{DorisSink, DorisSinkConfig};
use wp_model_core::model::{DataField, DataRecord};

/// 创建测试用的 DataRecord（与 wp_nginx 表结构完全匹配）
fn create_sample_record(id: i64) -> DataRecord {
    let mut record = DataRecord::default();
    // 按照表结构顺序添加字段
    // BIGINT - 事件唯一ID
    record.append(DataField::from_digit("wp_event_id", id));
    // STRING - 数据来源标识
    record.append(DataField::from_chars(
        "wp_src_key",
        format!("perf_test_{}", id),
    ));
    // STRING - 客户端IP
    record.append(DataField::from_chars("sip", "192.168.1.100"));
    // STRING - 原始时间字符串
    record.append(DataField::from_chars("timestamp", "2024-03-02 10:00:00"));
    // STRING - HTTP请求行
    record.append(DataField::from_chars(
        "http/request",
        format!("GET /api/test/{} HTTP/1.1", id),
    ));
    // SMALLINT - HTTP状态码（使用数值类型）
    record.append(DataField::from_digit("status", 200));
    // INT - 响应大小(byte)（使用数值类型）
    record.append(DataField::from_digit("size", 1024));
    // STRING - 来源页面
    record.append(DataField::from_chars("referer", "https://example.com/test"));
    // STRING - User-Agent
    record.append(DataField::from_chars(
        "http/agent",
        "Mozilla/5.0 (Performance Test)",
    ));
    record
}

/// 创建测试用的 DorisSink
async fn create_test_sink() -> DorisSink {
    let mut headers = std::collections::HashMap::new();
    // 设置最大过滤比例为 0（不允许任何数据被过滤）
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

    DorisSink::new(config).await.unwrap()
}

/// 调试测试：打印实际发送的 JSON 格式
#[tokio::test]
#[ignore = "调试用，查看 JSON 格式"]
async fn debug_print_json_format() {
    println!("\n========================================");
    println!("调试：查看实际发送的 JSON 格式");
    println!("========================================\n");

    // 创建一条测试记录
    let record = create_sample_record(12345);

    println!("生成的记录字段:");
    for field in &record.items {
        println!("  {} = {}", field.get_name(), field.get_value());
    }
    println!("\n========================================\n");
}

/// 调试测试：尝试发送单条记录
#[tokio::test]
#[ignore = "调试用，测试单条记录发送"]
async fn debug_send_single_record() {
    println!("\n========================================");
    println!("调试：测试发送单条记录");
    println!("========================================\n");

    let mut sink = create_test_sink().await;

    // 创建一条测试记录
    let record = create_sample_record(99999);
    let records = vec![std::sync::Arc::new(record)];

    println!("📤 尝试发送 1 条记录...");
    println!("   记录 ID: 99999");

    match sink.sink_records(records).await {
        Ok(_) => {
            println!("✅ 发送成功！");
            println!("\n💡 验证数据是否到达 Doris:");
            println!("   SELECT * FROM test_db.wp_nginx WHERE wp_event_id = 99999;");
        }
        Err(e) => {
            eprintln!("❌ 发送失败: {}", e);
            eprintln!("\n💡 可能的原因：");
            eprintln!("1. 表结构不匹配 - 检查字段名称和类型");
            eprintln!("2. 数据类型错误 - status 和 size 必须是数值");
            eprintln!("3. Doris 配置的 max_filter_ratio 太严格");
            eprintln!("4. 网络连接问题");
            eprintln!("\n建议：");
            eprintln!("- 运行 debug_print_json_format 查看 JSON 格式");
            eprintln!("- 检查 Doris BE 日志: tail -f /path/to/doris/be/log/be.INFO");
            eprintln!(
                "- 查询 Doris 导入历史: SELECT * FROM information_schema.loads ORDER BY create_time DESC LIMIT 10;"
            );
        }
    }

    println!("\n========================================\n");
}

/// 调试测试：发送小批量数据并查看详细响应
#[tokio::test]
#[ignore = "调试用，测试小批量发送"]
async fn debug_send_small_batch() {
    println!("\n========================================");
    println!("调试：测试发送 10 条记录");
    println!("========================================\n");

    // 启用详细日志
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .try_init();

    let mut sink = create_test_sink().await;

    // 创建 10 条测试记录
    let records: Vec<Arc<DataRecord>> = (1..=10)
        .map(|i| Arc::new(create_sample_record(88800 + i)))
        .collect();

    println!("📤 尝试发送 10 条记录 (ID: 88801-88810)...");
    println!("   查看日志中的 loaded 和 filtered 数量");
    println!();

    match sink.sink_records(records).await {
        Ok(_) => {
            println!("\n✅ 发送成功！");
            println!("\n💡 检查日志输出中的:");
            println!("   - number_loaded_rows: 应该是 10");
            println!("   - number_filtered_rows: 应该是 0");
            println!("\n💡 如果 filtered_rows > 0，说明数据被过滤了");
            println!("   常见原因：");
            println!("   1. 字段类型不匹配");
            println!("   2. 数据格式错误");
            println!("   3. 违反表约束");
            println!("\n💡 验证数据是否到达 Doris:");
            println!(
                "   SELECT COUNT(*) FROM test_db.wp_nginx WHERE wp_event_id BETWEEN 88801 AND 88810;"
            );
            println!("   -- 应该返回 10");
        }
        Err(e) => {
            eprintln!("\n❌ 发送失败: {}", e);
        }
    }

    println!("\n========================================\n");
}

/// 测试1: 单个异步任务发送 10w 数据
#[tokio::test]
#[ignore = "需要运行 Doris 实例，手动执行"]
async fn test_single_async_send_1m_records() {
    println!("\n========================================");
    println!("测试1: 单个异步任务发送 10w 数据");
    println!("========================================\n");

    let total_records = 100_000; // 10万条
    let batch_size = 1_000; // 每批1千条

    // 创建 sink
    println!("📦 创建 DorisSink 实例...");
    let mut sink = create_test_sink().await;
    println!("✅ Sink 创建完成\n");

    // 开始计时
    println!("🚀 开始发送数据...");
    let start = Instant::now();

    let mut sent = 0;
    let mut batch_count = 0;
    let mut error_count = 0;
    let mut success_count = 0;

    while sent < total_records {
        let current_batch = batch_size.min(total_records - sent);

        // 创建一批数据
        let records: Vec<Arc<DataRecord>> = (0..current_batch)
            .map(|i| Arc::new(create_sample_record((sent + i) as i64)))
            .collect();

        // 发送数据
        match sink.sink_records(records).await {
            Ok(_) => {
                sent += current_batch;
                batch_count += 1;
                success_count += 1;

                if batch_count % 10 == 0 {
                    let elapsed = start.elapsed().as_secs_f64();
                    let throughput = sent as f64 / elapsed;
                    println!(
                        "  已发送: {}/{} ({:.1}%) | 吞吐量: {:.0} 条/秒",
                        sent,
                        total_records,
                        (sent as f64 / total_records as f64) * 100.0,
                        throughput
                    );
                }

                // 重置错误计数
                error_count = 0;
            }
            Err(e) => {
                error_count += 1;
                eprintln!("❌ 批次 {} 发送失败: {}", batch_count + 1, e);

                // 只在前几次错误时打印详细信息
                if error_count <= 3 {
                    eprintln!("   批次大小: {}", current_batch);
                    eprintln!("   已发送: {}/{}", sent, total_records);
                    eprintln!("   成功批次: {}", success_count);
                }

                // 如果连续失败太多次，停止测试
                if error_count >= 5 {
                    eprintln!("\n❌ 连续失败 {} 次，停止测试", error_count);
                    eprintln!("💡 建议：");
                    eprintln!("   1. 检查 Doris 服务状态");
                    eprintln!("   2. 查看 Doris BE 日志");
                    eprintln!("   3. 减小 batch_size 或降低发送速度");
                    break;
                }

                // 失败后等待一下再继续
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }
    }

    let duration = start.elapsed();

    // 输出结果
    println!("\n========================================");
    println!("测试结果:");
    println!("========================================");
    println!("总记录数: {}", sent);
    println!("成功批次: {}", success_count);
    println!("失败批次: {}", batch_count - success_count);
    println!("总耗时: {:.2}秒", duration.as_secs_f64());

    if sent > 0 {
        println!("吞吐量: {:.0} 条/秒", sent as f64 / duration.as_secs_f64());
        println!(
            "EPS (Events Per Second): {:.0}",
            sent as f64 / duration.as_secs_f64()
        );
        println!(
            "平均延迟: {:.2}ms/批次",
            duration.as_millis() as f64 / success_count as f64
        );
    }

    println!("========================================\n");

    // 验证建议
    if sent > 0 {
        println!("💡 验证数据是否到达 Doris:");
        println!("   SELECT COUNT(*) FROM test_db.wp_nginx WHERE wp_src_key LIKE 'perf_test_%';");
        println!("   -- 应该返回 {}", sent);
    }
}

/// 测试2: 多个异步任务并发发送 100w 数据
#[tokio::test]
#[ignore = "需要运行 Doris 实例，手动执行"]
async fn test_concurrent_async_send_1m_records() {
    println!("\n========================================");
    println!("测试2: 多个异步任务并发发送 100w 数据");
    println!("========================================\n");

    let total_records = 1_000_000;
    let thread_count = 10; // 10个并发任务
    let batch_size = 1_0000; // 每批1万条
    let records_per_thread = total_records / thread_count;

    // 预先创建所有 sink 实例
    println!("📦 创建 {} 个 DorisSink 实例...", thread_count);
    let mut sinks = Vec::new();
    for i in 0..thread_count {
        let sink = create_test_sink().await;
        sinks.push(sink);
        if (i + 1) % 5 == 0 {
            println!("  已创建: {}/{}", i + 1, thread_count);
        }
    }
    println!("✅ 所有 Sink 创建完成\n");

    // 开始计时
    println!("🚀 开始并发发送数据...");
    let start = Instant::now();

    let mut handles = vec![];

    // 创建并发任务
    for (thread_id, mut sink) in sinks.into_iter().enumerate() {
        let handle = tokio::spawn(async move {
            let mut sent = 0;
            let mut batch_count = 0;

            while sent < records_per_thread {
                let current_batch = batch_size.min(records_per_thread - sent);

                let records: Vec<Arc<DataRecord>> = (0..current_batch)
                    .map(|i| {
                        Arc::new(create_sample_record(
                            (thread_id * records_per_thread + sent + i) as i64,
                        ))
                    })
                    .collect();

                match sink.sink_records(records).await {
                    Ok(_) => {
                        sent += current_batch;
                        batch_count += 1;
                    }
                    Err(e) => {
                        eprintln!("❌ 线程 {} 发送失败: {}", thread_id, e);
                        break;
                    }
                }
            }

            (thread_id, sent, batch_count)
        });
        handles.push(handle);
    }

    // 等待所有任务完成
    let mut total_sent = 0;
    let mut total_batches = 0;

    for handle in handles {
        match handle.await {
            Ok((thread_id, sent, batches)) => {
                println!(
                    "  线程 {} 完成: 发送 {} 条记录，{} 批次",
                    thread_id, sent, batches
                );
                total_sent += sent;
                total_batches += batches;
            }
            Err(e) => {
                eprintln!("❌ 线程执行失败: {}", e);
            }
        }
    }

    let duration = start.elapsed();

    // 输出结果
    println!("\n========================================");
    println!("测试结果:");
    println!("========================================");
    println!("并发任务数: {}", thread_count);
    println!("总记录数: {}", total_sent);
    println!("总耗时: {:.2}秒", duration.as_secs_f64());
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
        "每个任务 EPS: {:.0}",
        (total_sent as f64 / thread_count as f64) / duration.as_secs_f64()
    );
    println!("========================================\n");
}
