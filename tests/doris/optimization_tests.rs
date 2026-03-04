/// 测试 Doris Sink 优化功能
use wp_connectors::doris::{DorisSink, DorisSinkConfig};
use wp_model_core::model::{DataField, DataRecord};

/// 创建测试配置
fn create_test_config() -> DorisSinkConfig {
    DorisSinkConfig::new(
        "http://localhost:8040".to_string(),
        "test_db".to_string(),
        "test_table".to_string(),
        "root".to_string(),
        "".to_string(),
        Some(30),
        Some(3),
        None,
    )
}

/// 创建测试记录
#[allow(dead_code)]
fn create_test_record(id: i64) -> DataRecord {
    let mut record = DataRecord::default();
    record.append(DataField::from_digit("id", id));
    record.append(DataField::from_chars("name", &format!("user_{}", id)));
    record.append(DataField::from_chars(
        "email",
        &format!("user_{}@example.com", id),
    ));
    record
}

#[tokio::test]
async fn test_compression_config() {
    let config = create_test_config();

    // 验证默认配置
    assert!(config.enable_compression, "压缩应该默认启用");
    assert_eq!(config.compression_level, 6, "默认压缩级别应该是 6");
    assert_eq!(
        config.compression_threshold, 1024,
        "默认压缩阈值应该是 1024"
    );
}

#[tokio::test]
async fn test_batch_config() {
    let config = create_test_config();

    // 验证批量配置
    assert_eq!(config.min_batch_size, 100, "默认最小批量大小应该是 100");
    assert_eq!(config.max_batch_size, 10000, "默认最大批量大小应该是 10000");
    assert_eq!(
        config.max_batch_bytes,
        10 * 1024 * 1024,
        "默认最大批量字节数应该是 10 MB"
    );
}

#[tokio::test]
async fn test_connection_pool_config() {
    let config = create_test_config();

    // 验证连接池配置
    assert_eq!(
        config.pool_max_idle_per_host, 20,
        "默认每主机最大空闲连接数应该是 20"
    );
    assert_eq!(
        config.pool_idle_timeout_secs, 90,
        "默认空闲连接超时应该是 90 秒"
    );
}

#[tokio::test]
async fn test_sink_creation_with_optimizations() {
    let config = create_test_config();
    let result = DorisSink::new(config).await;

    // 验证 sink 可以成功创建
    assert!(result.is_ok(), "Sink 应该能够成功创建");
}

#[tokio::test]
async fn test_label_generation() {
    let config = create_test_config();
    let sink = DorisSink::new(config).await.unwrap();

    // 生成两个 label
    let label1 = sink.generate_label().unwrap();
    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    let label2 = sink.generate_label().unwrap();

    // 验证 label 唯一性
    assert_ne!(label1, label2, "Labels 应该是唯一的");
    assert!(label1.starts_with("doris_load_"), "Label 应该有正确的前缀");
}

#[test]
fn test_config_serialization() {
    let config = create_test_config();

    // 测试序列化
    let json = serde_json::to_string(&config).unwrap();
    assert!(json.contains("enable_compression"), "JSON 应该包含压缩配置");
    assert!(json.contains("min_batch_size"), "JSON 应该包含批量配置");

    // 测试反序列化
    let deserialized: DorisSinkConfig = serde_json::from_str(&json).unwrap();
    assert_eq!(config, deserialized, "序列化和反序列化应该保持一致");
}
