use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::sync::Arc;
use tokio::runtime::Runtime;
use wp_connector_api::AsyncRecordSink;
use wp_connectors::doris::{DorisSink, DorisSinkConfig};
use wp_model_core::model::{DataField, DataRecord};

/// 创建测试用的 DataRecord
fn create_sample_record(id: i64) -> DataRecord {
    let mut record = DataRecord::default();
    record.append(DataField::from_digit("wp_event_id", id));
    record.append(DataField::from_chars("wp_src_key", &format!("user_{}", id)));
    record.append(DataField::from_chars("sip", "111"));
    record.append(DataField::from_chars("timestamp", "111"));
    record.append(DataField::from_chars(
        "http/request",
        &format!("user_{}@example.com", id),
    ));
    record.append(DataField::from_chars("status", &format!("user_{}", id)));
    record.append(DataField::from_chars("size", "111"));
    record.append(DataField::from_chars("referer", "111"));
    record.append(DataField::from_chars(
        "http/agent",
        &format!("user_{}@example.com", id),
    ));
    record
}

/// 创建测试用的 DorisSink
async fn create_test_sink() -> DorisSink {
    let config = DorisSinkConfig::new(
        "http://localhost:8040".to_string(),
        "test_db".to_string(),
        "wp_nginx".to_string(),
        "root".to_string(),
        "".to_string(),
        Some(30),
        Some(3),
        None,
    );

    DorisSink::new(config).await.unwrap()
}

/// 清空测试表数据并触发 compaction
async fn truncate_and_compact_table() {
    use reqwest::Client;
    
    let client = Client::new();
    
    // 1. 清空表数据
    println!("🧹 清空测试表数据...");
    let sql = "TRUNCATE TABLE test_db.wp_nginx";
    let response = client
        .post("http://localhost:8040/api/test_db/_query")
        .basic_auth("root", Some(""))
        .header("Content-Type", "application/json")
        .body(format!(r#"{{"sql":"{}"}}"#, sql))
        .send()
        .await;
    
    match response {
        Ok(resp) => {
            if resp.status().is_success() {
                println!("✅ 已清空测试表数据");
            } else {
                eprintln!("⚠️  清空表失败: status={}", resp.status());
            }
        }
        Err(e) => {
            eprintln!("⚠️  清空表请求失败: {}", e);
        }
    }
    
    // 2. 触发 compaction（合并 tablet 版本）
    println!("🔄 触发 tablet compaction...");
    
    // 使用 Admin API 触发 compaction
    // 注意：这需要知道 tablet_id，从错误信息中我们知道是 1772438003878
    let tablet_id = "1772438003878";
    let compact_url = format!("http://172.20.80.3:8040/api/compaction/run?tablet_id={}&compact_type=cumulative", tablet_id);
    
    match client.post(&compact_url).send().await {
        Ok(resp) => {
            if resp.status().is_success() {
                println!("✅ 已触发 compaction");
            } else {
                eprintln!("⚠️  触发 compaction 失败: status={}", resp.status());
            }
        }
        Err(e) => {
            eprintln!("⚠️  触发 compaction 请求失败: {}", e);
        }
    }
    
    // 3. 等待 compaction 完成
    println!("⏳ 等待 compaction 完成（10秒）...");
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    println!("✅ 准备开始测试");
}

/// 基准测试：多线程并发发送 - 测试并发性能
fn bench_concurrent_send(c: &mut Criterion) {
    use std::sync::Arc as StdArc;
    use tokio::sync::Mutex;

    let rt = Runtime::new().unwrap();
    
    // 测试前清空表数据并触发 compaction
    println!("🧹 准备测试环境...");
    rt.block_on(async {
        truncate_and_compact_table().await;
    });
    
    // 调整参数
    let total_records = 10_000;
    let thread_count = 10;
    let batch_size = 1000;
    let records_per_thread = total_records / thread_count;

    // 预先创建所有 sink 实例（每个线程一个）
    println!("📦 创建 {} 个 sink 实例...", thread_count);
    let sinks: Vec<StdArc<Mutex<DorisSink>>> = rt.block_on(async {
        let mut sinks = Vec::new();
        for _ in 0..thread_count {
            let sink = create_test_sink().await;
            sinks.push(StdArc::new(Mutex::new(sink)));
        }
        sinks
    });
    println!("✅ Sink 实例创建完成");

    c.bench_function(
        &format!("doris_concurrent_{}threads_{}k_records", thread_count, total_records / 1000),
        |b| {
            b.iter(|| {
                rt.block_on(async {
                    let mut handles = vec![];

                    // 每个线程使用预先创建的 sink
                    for thread_id in 0..thread_count {
                        let sink = StdArc::clone(&sinks[thread_id]);
                        
                        let handle = tokio::spawn(async move {
                            let mut sent = 0;
                            while sent < records_per_thread {
                                let current_batch = batch_size.min(records_per_thread - sent);
                                let records: Vec<Arc<DataRecord>> = (0..current_batch)
                                    .map(|i| {
                                        Arc::new(create_sample_record(
                                            (thread_id * records_per_thread + sent + i) as i64,
                                        ))
                                    })
                                    .collect();

                                let mut sink_guard = sink.lock().await;
                                match sink_guard.sink_records(records).await {
                                    Ok(_) => {},
                                    Err(e) => {
                                        eprintln!("Warning: sink_records failed: {}", e);
                                        if e.to_string().contains("exceed limit") {
                                            break;
                                        }
                                    }
                                }
                                drop(sink_guard);
                                sent += current_batch;
                            }
                        });
                        handles.push(handle);
                    }

                    // 等待所有线程完成
                    for handle in handles {
                        let _ = handle.await;
                    }
                })
            });
        },
    );
}

criterion_group!(benches, bench_concurrent_send);
criterion_main!(benches);
