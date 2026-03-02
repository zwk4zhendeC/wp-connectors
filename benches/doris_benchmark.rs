use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};
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

/// 基准测试：批量记录写入

fn bench_batch_records(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    // 预先创建一个 sink 实例（在测试循环外）
    let mut sink = rt.block_on(async { create_test_sink().await });
    c.bench_function("doris_sink_10000_records", |b| {
        b.iter_batched(
            || {
                // Setup: 创建测试数据（每次迭代都会调用）
                let records: Vec<Arc<DataRecord>> = (0..10000)
                    .map(|i| Arc::new(create_sample_record(i)))
                    .collect();
                records
            },
            |records| {
                // Routine: 被测试的代码（使用 setup 返回的数据）
                for _i in 0..100 {
                    rt.block_on(async { 
                        black_box(sink.sink_records(records.clone()).await.unwrap())
                    })
                }
            },
            BatchSize::SmallInput,
        );
    });
}
// 配置基准测试组：严格控制迭代次数
// 使用 sample_size 控制样本数，禁用自动调整
criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)  // 10个样本
        .measurement_time(std::time::Duration::from_secs(10))  // 测量时间10秒，防止自动增加迭代
        .warm_up_time(std::time::Duration::from_secs(1));     // 预热1秒
    targets = bench_batch_records
}

criterion_main!(benches);
