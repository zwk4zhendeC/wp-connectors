# Tests Guide

`tests/` 目录包含项目的集成测试、性能测试，以及两类测试复用的公共工具。

## 目录结构

```text
tests/
├── common/
│   ├── component_tools.rs
│   └── sink/
│       ├── integration_runtime.rs
│       ├── performance_runtime.rs
│       └── sink_info.rs
├── doris/
│   ├── common.rs
│   ├── integration_tests.rs
│   ├── integration_tests.yml
│   ├── performance_tests.rs
│   └── performance_tests.yml
├── doris_tests.rs
```

## Common 工具

### `component_tools`

`tests/common/component_tools.rs` 提供测试依赖环境的统一抽象。

#### `ComponentTool`

`ComponentTool` 是异步 trait，负责组件生命周期管理，当前主要方法有：

- `pull_dependencies()`：拉取镜像或安装依赖
- `up()`：启动组件
- `down()`：停止组件
- `wait_started()`：等待组件启动完成
- `restart()`：重启组件
- `setup_and_up()`：完整启动流程，默认会执行 `pull -> up -> wait_started`

#### `DockerComposeTool`

`DockerComposeTool` 适合依赖 Docker Compose 的测试场景。

创建方式：

```rust
let tool = DockerComposeTool::new("tests/doris/integration_tests.yml")?;
```

常见用法：

```rust
tool.setup_and_up().await?;
tool.wait_started().await?;
tool.restart().await?;
tool.down().await?;
```

`wait_started()` 目前通过 `docker compose ps` 轮询服务是否进入 `running` 状态。

#### `ShellScriptTool`

`ShellScriptTool` 适合通过 shell 脚本准备环境的测试场景。

创建方式：

```rust
let tool = ShellScriptTool::new(
    "scripts/start.sh",
    "scripts/stop.sh",
)?;
```

如果需要安装依赖、显式就绪检查或自定义重启逻辑，可以提供可选脚本：

```rust
let tool = ShellScriptTool::new_with_options(
    "scripts/start.sh",
    "scripts/stop.sh",
    Some("scripts/install.sh"),
    Some("scripts/ready.sh"),
    ShellScriptRestart::Script("scripts/restart.sh"),
)?;
```

说明：

- `start` / `stop` 脚本是必选项
- `install` / `ready` 是可选项
- `restart` 使用 `ShellScriptRestart` 枚举配置：`Default` / `Script(...)` / `NoRestart`
- 如果没有 `ready` 脚本，`wait_started()` 会直接视为成功
- `ShellScriptRestart::Default` 会回退为 `stop() + start()`
- `ShellScriptRestart::NoRestart` 会跳过重启动作

### `sink_info`

`tests/common/sink/sink_info.rs` 定义了 `SinkInfo`，它是集成测试和性能测试共用的 sink 描述对象。

#### `SinkInfo`

`SinkInfo` 负责描述：

- 使用哪个 `SinkFactory`
- 可选的测试名称 `test_name`
- sink 的参数 `ParamMap`
- 初始化逻辑
- 必选的数量查询逻辑 `count_fn`
- 可选的 sink 级就绪检查逻辑 `wait_ready_fn`

创建方式：

```rust
let sink_info = SinkInfo::new(MySinkFactory, params, |_params| async { query_count().await })
    .with_test_name("basic_json")
    .with_async_init(|| async { init_database().await })
    .with_async_wait_ready(|_params| async { wait_sink_ready().await });
```

常见方法：

- `with_async_init(...)`
- `with_init_sh(...)`
- `with_test_name(...)`
- `with_async_wait_ready(...)`
- `init().await`
- `wait_ready().await`
- `count().await`

如果 sink 目标端需要额外探测才能确认可用，可以配置 `wait_ready_fn`；未配置时会默认跳过该步骤。
如果配置了 `test_name`，运行时会在遍历开始时输出 `kind + test_name + 编号` 形式的测试标识。

### `integration_runtime`

`tests/common/sink/integration_runtime.rs` 定义了 `SinkIntegrationRuntime`，用于标准集成测试。

它会执行以下流程：

1. 启动环境并等待组件就绪
2. 遍历 `SinkInfo`
3. 调用 `init()` 准备库表或依赖数据
4. 创建 sink
5. 发送固定数量测试数据（当前默认 3 条）
6. 调用 `wait_ready()` 确认 sink 目标端可用
7. 通过 `count_fn` 校验发送前后数量变化
8. 重启环境后执行 `wait_started() + wait_ready()`，再次发送并再次校验
9. 清理环境

典型用法：

```rust
let runtime = SinkIntegrationRuntime::new(component_tool, vec![sink_info]);
runtime.run().await?;
```

### `performance_runtime`

`tests/common/sink/performance_runtime.rs` 定义了 `SinkPerformanceRuntime`，用于性能测试。

它会执行以下流程：

1. 启动环境并等待组件就绪
2. 遍历 `SinkInfo`
3. 调用 `init()` 准备库表
4. 按配置创建多个 sink 和多个 Tokio 任务
5. 按批次生成测试数据并发送
6. 调用 `wait_ready()` 确认 sink 目标端可用
7. 定时打印性能指标：
   - 当前 EPS
   - 总体 EPS
   - 当前进程 CPU
   - 当前进程内存
   - 峰值 CPU / 峰值内存
8. 发送结束后会再次查询数量并校验增量

#### `SinkPerformanceConfig`

默认配置：

- `total_records = 2_000_000`
- `task_count = 4`
- `batch_size = 10_000`
- `progress_interval = 5s`

支持链式配置：

```rust
use std::time::Duration;

let config = SinkPerformanceConfig::new()
    .with_total_records(500_000)
    .with_task_count(8)
    .with_batch_size(5_000)
    .with_progress_interval(Duration::from_secs(2));
```

运行方式：

```rust
let runtime = SinkPerformanceRuntime::new(component_tool, vec![sink_info], config);
runtime.run().await?;
```

## 如何编写集成测试

推荐步骤：

1. 在具体 sink 目录下创建测试文件，例如 `tests/doris/integration_tests.rs`
2. 如果当前 connector 有公共初始化/建表逻辑，优先抽到同目录 `common.rs`
3. 创建 `ComponentTool`，例如 `DockerComposeTool`
4. 构造 `SinkInfo`
5. 给 `SinkInfo` 配置：
    - 参数 `ParamMap`
    - `with_async_init(...)`
    - 必选 `count_fn`
    - 可选 `with_async_wait_ready(...)`
6. 使用 `SinkIntegrationRuntime` 执行

示例：

```rust
#[tokio::test]
async fn test_xxx_sink_full_integration() -> anyhow::Result<()> {
    let component_tool = DockerComposeTool::new("tests/doris/integration_tests.yml")?;
    let sink_info = SinkInfo::new(
        DorisSinkFactory,
        create_doris_test_config(),
        |_params| async { query_table_count().await },
    )
        .with_async_init(|| async { init_doris_database().await })
        .with_async_wait_ready(|_params| async { wait_for_sink_ready().await });

    let runtime = SinkIntegrationRuntime::new(component_tool, vec![sink_info]);
    runtime.run().await
}
```

## 如何编写性能测试

推荐步骤：

1. 在具体 sink 目录下创建性能测试文件，例如 `tests/doris/performance_tests.rs`
2. 优先复用该目录已有的 `common.rs`
3. 创建 `ComponentTool`
4. 创建 `SinkInfo`
   - 必须配置 `count_fn`，用于发送前后数量校验
   - 如有需要，可额外配置 `with_async_wait_ready(...)`
5. 创建 `SinkPerformanceConfig`
6. 使用 `SinkPerformanceRuntime` 执行
7. 给测试打上 `#[ignore]`，默认不进入 CI

示例：

```rust
#[tokio::test]
#[ignore = "性能测试默认不在 CI 中运行，请手动执行"]
async fn test_doris_sink_performance() -> anyhow::Result<()> {
    let component_tool = DockerComposeTool::new("tests/doris/performance_tests.yml")?;
    let sink_info = SinkInfo::new(
        DorisSinkFactory,
        create_doris_test_config(),
        |_params| async { query_table_count().await },
    )
        .with_async_init(|| async { init_doris_database().await })
        .with_async_wait_ready(|_params| async { wait_for_sink_ready().await });

    let config = SinkPerformanceConfig::new()
        .with_total_records(2_000_000)
        .with_task_count(4)
        .with_batch_size(10_000);

    let runtime = SinkPerformanceRuntime::new(component_tool, vec![sink_info], config);
    runtime.run().await
}
```

## 如何运行测试

### 运行全部单元测试

```bash
cargo test --lib --bins --doc --all-features
```

### 运行全部集成测试 target

当前顶层集成测试 target 主要有：

- `doris_tests`
- `http_tests`
- `kafka_tests`
- `common_kafka`

示例：

```bash
cargo test --package wp-connectors --test doris_tests --features doris -- --nocapture
```

### 运行 Doris 集成测试

```bash
cargo test --package wp-connectors --test doris_tests integration_tests::test_doris_sink_full_integration --features doris -- --exact --nocapture
```

Release 模式：

```bash
cargo test --release --package wp-connectors --test doris_tests integration_tests::test_doris_sink_full_integration --features doris -- --exact --nocapture
```

### 运行 Doris 性能测试

性能测试默认带 `#[ignore]`，需要显式加 `--ignored`。

```bash
cargo test --package wp-connectors --test doris_tests performance_tests::test_doris_sink_performance --features doris -- --exact --nocapture --ignored
```

Release 模式：

```bash
cargo test --release --package wp-connectors --test doris_tests performance_tests::test_doris_sink_performance --features doris -- --exact --nocapture --ignored
```

### 运行全部被忽略的 Doris 测试

```bash
cargo test --package wp-connectors --test doris_tests --features doris -- --ignored --nocapture --test-threads=1
```

### 运行 HTTP 集成测试

```bash
cargo test --package wp-connectors --test http_tests --features http integration_tests::test_http_sink_full_integration -- --exact --nocapture
```

### 运行 HTTP 性能测试

HTTP 性能测试默认带 `#[ignore]`，需要显式加 `--ignored`。

```bash
cargo test --package wp-connectors --test http_tests --features http performance_tests::test_http_sink_performance -- --exact --nocapture --ignored
```

## 编写建议

- 集成测试优先验证正确性，性能测试优先验证吞吐与资源占用
- 可复用的 connector 初始化逻辑，优先抽到 `tests/<connector>/common.rs`
- 需要 Docker 的测试，优先通过 `DockerComposeTool` 管理依赖环境
- 性能测试默认使用 `#[ignore]`，避免进入普通 CI
- 性能测试推荐使用 `--release` 模式运行
