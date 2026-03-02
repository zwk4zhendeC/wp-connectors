# Doris Sink Benchmarks

这个目录包含 Doris Sink 的性能基准测试。

## 运行基准测试

### 运行所有基准测试

```bash
cargo bench --bench doris_benchmark
```

### 运行特定的基准测试

```bash
# 只运行 label 生成测试
cargo bench --bench doris_benchmark -- label

# 只运行 JSON 序列化测试
cargo bench --bench doris_benchmark -- json

# 只运行批量写入测试
cargo bench --bench doris_benchmark -- sink
```

### 保存基准测试结果

```bash
# 保存基线结果
cargo bench --bench doris_benchmark -- --save-baseline my-baseline

# 与基线对比
cargo bench --bench doris_benchmark -- --baseline my-baseline
```

## 基准测试项目

1. **bench_label_generation**: 测试 label 生成的性能
2. **bench_json_serialization**: 测试 100 条记录的 JSON 序列化性能
3. **bench_batch_records**: 测试 1000 条记录的批量写入性能

## 查看结果

基准测试结果会保存在 `target/criterion/` 目录下，包含：
- HTML 报告：`target/criterion/report/index.html`
- 详细数据：各个测试的统计信息

## 注意事项

- 基准测试使用模拟的 Doris Sink（不实际连接 Doris）
- 测试结果受系统负载影响，建议在空闲系统上运行
- 首次运行会较慢，因为需要编译依赖
