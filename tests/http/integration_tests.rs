use anyhow::Result;
use wp_connectors::http::HttpSinkFactory;

use crate::common::{
    component_tools::{ShellScriptRestart, ShellScriptTool},
    sink::{integration_runtime::SinkIntegrationRuntime, sink_info::SinkInfo},
};
use crate::http_common::{
    create_http_integration_scenarios, query_http_count, wait_for_http_ready,
};

#[tokio::test]
async fn test_http_sink_full_integration() -> Result<()> {
    let tool = ShellScriptTool::new_with_options(
        "tests/http/start_server.sh",
        "tests/http/stop_server.sh",
        Some("tests/http/install_deps.sh"),
        Some("tests/http/wait_ready.sh"),
        ShellScriptRestart::NoRestart,
    )?;

    let sink_infos = create_http_integration_scenarios()
        .into_iter()
        .map(|(test_name, params)| {
            SinkInfo::new(HttpSinkFactory, params, query_http_count)
                .with_test_name(test_name)
                .with_async_init(|| async { wait_for_http_ready().await })
                .with_async_wait_ready(|_params| async { wait_for_http_ready().await })
        })
        .collect();

    let runtime = SinkIntegrationRuntime::new(tool, sink_infos);
    runtime.run().await
}
