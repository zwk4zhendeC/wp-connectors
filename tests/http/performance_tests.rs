use anyhow::Result;
use std::time::Duration;
use wp_connectors::http::HttpSinkFactory;

use crate::common::{
    component_tools::{ShellScriptRestart, ShellScriptTool},
    sink::{
        performance_runtime::{SinkPerformanceConfig, SinkPerformanceRuntime},
        sink_info::SinkInfo,
    },
};
use crate::http_common::{create_http_test_config, query_http_count, wait_for_http_ready};

#[tokio::test]
#[ignore = "性能测试默认不在 CI 中运行，请手动执行"]
async fn test_http_sink_performance() -> Result<()> {
    let tool = ShellScriptTool::new_with_options(
        "tests/http/start_server.sh",
        "tests/http/stop_server.sh",
        Some("tests/http/install_deps.sh"),
        Some("tests/http/wait_ready.sh"),
        ShellScriptRestart::Default,
    )?;

    let sink_info = SinkInfo::new(
        HttpSinkFactory,
        create_http_test_config("/gzip/ingest/json", "json", "gzip", None, None, 10_000),
        query_http_count,
    )
    .with_test_name("gzip_json")
    .with_async_init(|| async { wait_for_http_ready().await })
    .with_async_wait_ready(|_params| async { wait_for_http_ready().await });

    let config = SinkPerformanceConfig::new()
        .with_total_records(2_000_000)
        .with_task_count(4)
        .with_batch_size(10_000)
        .with_progress_interval(Duration::from_secs(5));

    let runtime = SinkPerformanceRuntime::new(tool, vec![sink_info], config);
    runtime.run().await
}
