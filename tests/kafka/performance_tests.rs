#![cfg(feature = "kafka")]

use anyhow::Result;
use wp_connectors::kafka::KafkaSinkFactory;

use crate::common::{
    component_tools::DockerComposeTool,
    sink::{
        performance_runtime::{SinkPerformanceConfig, SinkPerformanceRuntime},
        sink_info::SinkInfo,
    },
};
use crate::kafka_common::{
    create_kafka_performance_config, init_kafka_topic_with_params, wait_for_kafka_ready,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "性能测试默认不在 CI 中运行，请手动执行"]
async fn test_kafka_sink_performance() -> Result<()> {
    let docker_tool = DockerComposeTool::new("tests/kafka/component/docker-compose.yml")?;

    let params = create_kafka_performance_config();
    let sink_info = SinkInfo::new(KafkaSinkFactory, params.clone())
        .with_test_name("baseline")
        .with_async_init(move || {
            let params = params.clone();
            async move { init_kafka_topic_with_params(params).await }
        })
        .with_async_wait_ready(|_params| async move { wait_for_kafka_ready().await });

    let config = SinkPerformanceConfig::default()
        .with_total_records(1_000_000)
        .with_batch_size(5_000)
        .with_task_count(4);

    let runtime = SinkPerformanceRuntime::new(docker_tool, vec![sink_info], config);
    runtime.run().await
}
