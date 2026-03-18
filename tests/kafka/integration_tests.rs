#![cfg(feature = "kafka")]

use anyhow::Result;
use wp_connectors::kafka::KafkaSinkFactory;

use crate::common::{
    component_tools::DockerComposeTool,
    sink::{integration_runtime::SinkIntegrationRuntime, sink_info::SinkInfo},
};
use crate::kafka_common::{
    create_kafka_test_config, init_kafka_topic_with_params, query_topic_count,
    wait_for_kafka_ready,
};

#[tokio::test]
async fn test_kafka_sink_full_integration() -> Result<()> {
    let docker_tool = DockerComposeTool::new("tests/kafka/component/docker-compose.yml")?;
    let params = create_kafka_test_config();

    let sink_info = SinkInfo::new(KafkaSinkFactory, params.clone())
        .with_test_name("baseline")
        .with_async_count_fn(|params| async move { query_topic_count(params).await })
        .with_async_init(move || {
            let params = params.clone();
            async move { init_kafka_topic_with_params(params).await }
        })
        .with_async_wait_ready(|_params| async move { wait_for_kafka_ready().await });

    let runtime = SinkIntegrationRuntime::new(docker_tool, vec![sink_info]);
    runtime.run().await
}
