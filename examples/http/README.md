# HTTP Sink 示例和测试

本目录包含 HTTP Sink 的示例程序和并发性能测试。

## 文件说明

- `http_sink_example.rs` - HTTP Sink 基础使用示例
- `http_sink_concurrent_test.rs` - HTTP Sink 并发性能测试

## 前置准备

### 1. 启动测试 HTTP 服务器

在项目根目录运行 Python 测试服务器：

```bash
# 使用项目提供的测试服务器（监听 8080 端口）
python3 test_server.py 8080
```

或者使用你自己的 HTTP 服务器，确保它：
- 监听 `localhost:8080`
- 接受 POST 请求到 `/ingest` 路径
- 返回 2xx 状态码表示成功

### 2. 编译项目

```bash
# 开发模式
cargo build --features http

# 发布模式（推荐用于性能测试）
cargo build --release --features http
```

## 运行示例

### 基础示例

演示 HTTP Sink 的基本功能，包括：
- 单条记录发送
- 批量记录发送
- 不同数据格式（JSON, NDJSON, CSV, KV）
- Gzip 压缩
- 自定义 HTTP 头
- Basic 认证

```bash
cargo run --example http_sink_example --features http
```

### 并发性能测试

测试 HTTP Sink 在不同格式和压缩选项下的并发性能：

```bash
# 开发模式（调试用）
cargo run --example http_sink_concurrent_test --features http

# 发布模式（性能测试）
cargo run --example http_sink_concurrent_test --features http --release
```

## 并发性能测试说明

### 测试场景

测试程序会依次运行以下 8 个场景：

**不压缩场景：**
1. JSON (无压缩)
2. NDJSON (无压缩)
3. CSV (无压缩)
4. KV (无压缩)

**Gzip 压缩场景：**
5. JSON (gzip)
6. NDJSON (gzip)
7. CSV (gzip)
8. KV (gzip)

### 测试参数

默认配置（可在代码中修改）：

```rust
const HTTP_ENDPOINT: &str = "http://localhost:8080/ingest";
const TOTAL_RECORDS: usize = 100_000;  // 每场景 10 万条记录
const TASK_COUNT: usize = 4;           // 4 个并发任务
const BATCH_SIZE: usize = 1000;        // 每批 1000 条
const TIMEOUT_SECS: u64 = 30;          // 30 秒超时
const MAX_RETRIES: i32 = 3;            // 最多重试 3 次
```

### 输出示例

```
╔════════════════════════════════════════╗
║   HTTP Sink 并发性能测试               ║
╚════════════════════════════════════════╝

测试配置:
  HTTP 端点: http://localhost:8080/ingest
  总记录数: 100000 (每场景)
  并发任务数: 4
  批次大小: 1000
  超时时间: 30s
  最大重试: 3 次

将运行 8 个测试场景

▶ 场景 1/8: JSON (无压缩)

========================================
测试场景: JSON (无压缩)
========================================
  格式: json
  压缩: none
  总记录数: 100000
  并发任务数: 4
  批次大小: 1000

  [Task 0] 已发送 10000/25000 条记录 (5234 records/s)
  [Task 1] 已发送 10000/25000 条记录 (5198 records/s)
  ...

----------------------------------------
测试结果:
  成功发送: 100000 条记录
  失败任务: 0 个
  总耗时: 19.23s
  平均吞吐量: 5200 records/s
  平均延迟: 0.19ms/record
----------------------------------------

...（其他场景）

╔════════════════════════════════════════╗
║   测试总结                             ║
╚════════════════════════════════════════╝

各场景耗时:
  JSON (无压缩): 19.23s
  NDJSON (无压缩): 18.95s
  CSV (无压缩): 20.12s
  KV (无压缩): 18.76s
  JSON (gzip): 21.45s
  NDJSON (gzip): 20.89s
  CSV (gzip): 22.34s
  KV (gzip): 21.12s

总耗时: 163.86s

✅ 所有测试完成！
```

## 性能优化建议

### 1. 使用发布模式

```bash
cargo run --example http_sink_concurrent_test --features http --release
```

发布模式比开发模式快 10-100 倍。

### 2. 调整并发参数

根据你的服务器性能调整：

```rust
const TASK_COUNT: usize = 8;      // 增加并发任务数
const BATCH_SIZE: usize = 5000;   // 增加批次大小
```

### 3. 使用压缩

对于网络带宽受限的场景，gzip 压缩可以显著减少传输数据量：

```rust
compression: "gzip"  // 通常可减少 60-80% 的数据量
```

### 4. 选择合适的格式

- **JSON**: 最通用，易于调试
- **NDJSON**: 适合流式处理和批量操作
- **CSV**: 数据量最小，适合表格数据
- **KV**: 简单键值对，适合日志场景

## 故障排查

### 连接被拒绝

```
Error: request failed: error sending request for url (http://localhost:8080/ingest): error trying to connect: tcp connect error: Connection refused
```

**解决方案**: 确保测试服务器正在运行：
```bash
python3 test_server.py 8080
```

### 超时错误

```
Error: request failed: operation timed out
```

**解决方案**:
1. 增加超时时间：`const TIMEOUT_SECS: u64 = 60;`
2. 减少批次大小：`const BATCH_SIZE: usize = 500;`
3. 检查服务器性能

### 连续失败

```
Task 0 连续失败 3 次，停止任务
```

**解决方案**:
1. 检查服务器日志
2. 增加重试次数：`const MAX_RETRIES: i32 = 5;`
3. 增加重试延迟：`const ERROR_RETRY_DELAY_MS: u64 = 500;`

## 自定义测试

### 修改测试端点

编辑 `http_sink_concurrent_test.rs`:

```rust
const HTTP_ENDPOINT: &str = "https://your-api.com/webhook";
```

### 添加认证

修改 `create_test_sink` 函数：

```rust
let config = HttpSinkConfig::new(
    HTTP_ENDPOINT.to_string(),
    Some(HTTP_METHOD.to_string()),
    Some("username".to_string()),  // 添加用户名
    Some("password".to_string()),  // 添加密码
    None,
    Some(format.to_string()),
    None,
    Some(TIMEOUT_SECS),
    Some(MAX_RETRIES),
    Some(compression.to_string()),
);
```

### 添加自定义头

```rust
use std::collections::HashMap;

let mut headers = HashMap::new();
headers.insert("X-API-Key".to_string(), "your-api-key".to_string());
headers.insert("X-Custom-Header".to_string(), "value".to_string());

let config = HttpSinkConfig::new(
    HTTP_ENDPOINT.to_string(),
    Some(HTTP_METHOD.to_string()),
    None,
    None,
    Some(headers),  // 添加自定义头
    Some(format.to_string()),
    None,
    Some(TIMEOUT_SECS),
    Some(MAX_RETRIES),
    Some(compression.to_string()),
);
```

## 相关文档

- [HTTP Sink 设计文档](../../.kiro/specs/http-sink/design.md)
- [HTTP Sink 需求文档](../../.kiro/specs/http-sink/requirements.md)
- [HTTP Sink 实现任务](../../.kiro/specs/http-sink/tasks.md)
