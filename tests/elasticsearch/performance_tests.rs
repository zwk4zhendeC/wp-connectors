#![cfg(feature = "elasticsearch")]

use anyhow::Result;
use wp_connectors::elasticsearch::ElasticsearchSinkFactory;

use crate::common::{
    component_tools::DockerComposeTool,
    sink::{
        performance_runtime::{SinkPerformanceConfig, SinkPerformanceRuntime},
        sink_info::SinkInfo,
    },
};
use crate::elasticsearch_common::{
    create_elasticsearch_test_config, init_elasticsearch_index, query_index_count,
    wait_for_elasticsearch_sink_ready,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "性能测试默认不在 CI 中运行，请手动执行"]
async fn test_elasticsearch_sink_performance() -> Result<()> {
    let docker_tool = DockerComposeTool::new("tests/elasticsearch/component/docker-compose.yml")?;

    let sink_info = SinkInfo::new(ElasticsearchSinkFactory, create_elasticsearch_test_config())
        .with_test_name("baseline")
        .with_async_count_fn(|_params| async { query_index_count().await })
        .with_async_init(|| async { init_elasticsearch_index().await })
        .with_async_wait_ready(|_params| async { wait_for_elasticsearch_sink_ready().await });

    let config = SinkPerformanceConfig::default()
        .with_batch_size(8_192)
        .with_task_count(4);

    let runtime = SinkPerformanceRuntime::new(docker_tool, vec![sink_info], config);
    runtime.run().await
}
