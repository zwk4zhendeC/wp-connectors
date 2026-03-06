# HTTP Sink 示例和测试

本目录包含 HTTP Sink 的完整功能测试示例。

## 文件说明

- `http_sink_example.rs` - 完整的功能测试程序
- `http_sink_concurrent_test.rs` - 并发性能测试程序
- `test_server.py` - Python 测试服务器

## 快速开始

### 1. 启动测试服务器

```bash
python3 examples/http/test_server.py
```

服务器将在 `http://localhost:8080` 上监听，并提供以下接口：

- `POST /ingest/{format}` - 接收指定格式的数据（无认证，无压缩）
- `POST /auth/ingest/{format}` - 接收指定格式的数据（需要认证：root/root）
- `POST /gzip/ingest/{format}` - 接收 GZIP 压缩的数据
- `GET /count` - 查看所有格式的统计
- `GET /details/{format}` - 查看指定格式的最后 3 条数据

### 2. 运行功能测试

```bash
cargo run --example http_sink_example --features http
```

该测试将执行三类测试：

1. **基础数据接收**（无压缩，无认证）
   - 向 `/ingest/{format}` 发送 3 条记录
   - 测试所有 6 种格式：json, ndjson, csv, kv, raw, proto-text

2. **认证数据接收**（无压缩，需要认证）
   - 向 `/auth/ingest/{format}` 发送 3 条记录
   - 使用 Basic Auth（用户名：root，密码：root）
   - 测试所有 6 种格式

3. **GZIP 压缩数据接收**（GZIP 压缩，无认证）
   - 向 `/gzip/ingest/{format}` 发送 3 条记录
   - 数据使用 GZIP 压缩
   - 测试所有 6 种格式

### 3. 运行并发测试

```bash
cargo run --example http_sink_concurrent_test --features http
```

## 支持的数据格式

| 格式 | 说明 | 示例 |
|------|------|------|
| `json` | JSON 数组 | `[{"id":1,"name":"alice"},{"id":2,"name":"bob"}]` |
| `ndjson` | 换行分隔的 JSON | `{"id":1,"name":"alice"}\n{"id":2,"name":"bob"}` |
| `csv` | CSV 格式（带表头） | `id,name\n1,alice\n2,bob` |
| `kv` | 键值对格式 | `id=1 name=alice\nid=2 name=bob` |
| `raw` | 原始字段值（空格分隔） | `1 alice\n2 bob` |
| `proto-text` | Protocol Buffer 文本格式 | `id: 1\nname: "alice"\n\nid: 2\nname: "bob"` |

## 测试结果验证

### 查看统计信息

```bash
curl http://localhost:8080/count
```

预期输出：
```json
{
  "status": "success",
  "counts": {
    "json": 9,
    "ndjson": 9,
    "csv": 9,
    "kv": 9,
    "raw": 9,
    "proto-text": 9
  },
  "total": 54
}
```

每种格式应该接收到 9 条记录（3 个测试类别 × 3 条记录）。

### 查看特定格式的详细数据

```bash
curl http://localhost:8080/details/json
```

## 测试服务器功能

测试服务器会：

1. **接收数据** - 接收 HTTP POST 请求
2. **输出原始内容** - 在控制台打印接收到的原始数据
3. **解析数据** - 根据格式解析数据
4. **验证格式** - 如果解析失败则返回错误
5. **统计数据** - 成功解析后加入统计

## 故障排查

### 连接被拒绝

确保测试服务器正在运行：
```bash
python3 examples/http/test_server.py
```

### 认证失败

确保使用正确的用户名和密码：
- 用户名：`root`
- 密码：`root`

### 格式解析失败

检查测试服务器的控制台输出，查看详细的错误信息。

## 配置说明

HTTP Sink 支持以下配置参数：

- `endpoint` - HTTP(S) 端点 URL（必填）
- `method` - HTTP 方法（默认：POST）
- `username` - Basic Auth 用户名（可选）
- `password` - Basic Auth 密码（可选）
- `headers` - 自定义 HTTP 头（可选）
- `fmt` - 输出格式（默认：json）
- `batch_size` - 批量大小（默认：1）
- `timeout_secs` - 超时时间（默认：60 秒）
- `max_retries` - 最大重试次数（默认：3）
- `compression` - 压缩算法（默认：none，支持：gzip）

## 性能测试

并发测试程序 `http_sink_concurrent_test.rs` 可以测试 HTTP Sink 的并发性能：

```bash
cargo run --example http_sink_concurrent_test --features http
```

可以通过修改程序中的常量来调整测试参数：
- `TOTAL_RECORDS` - 总记录数
- `TASK_COUNT` - 并发任务数
- `BATCH_SIZE` - 每批次大小
