#![cfg(all(feature = "kafka", feature = "external_integration"))]

use std::time::Duration;

use anyhow::Result;
use wp_connectors::kafka::KafkaSourceFactory;

use crate::common::{
    component_tools::DockerComposeTool,
    source::{integration_runtime::SourceIntegrationRuntime, source_info::SourceInfo},
};
use crate::kafka_common::{
    create_kafka_source_config, init_kafka_topic_with_params, produce_topic_messages,
    unique_kafka_topic, wait_for_kafka_ready,
};

#[tokio::test]
#[ignore = "集成测试默认忽略，请按需手动执行"]
async fn test_kafka_source_basic_integration() -> Result<()> {
    let docker_tool = DockerComposeTool::new("tests/kafka/component/docker-compose.yml")?;

    let params = create_kafka_source_config(unique_kafka_topic("wp_kafka_source_basic"));
    let source_info = SourceInfo::new(KafkaSourceFactory, params.clone())
        .with_test_name("basic")
        .with_async_wait_ready(|_params| async move { wait_for_kafka_ready().await })
        .with_async_init(move || {
            let params = params.clone();
            async move { init_kafka_topic_with_params(params).await }
        })
        .with_async_input(|params| async move {
            produce_topic_messages(
                params,
                vec![
                    br#"{"seq":1,"case":"basic"}"#.to_vec(),
                    br#"{"seq":2,"case":"basic"}"#.to_vec(),
                    br#"{"seq":3,"case":"basic"}"#.to_vec(),
                ],
            )
            .await
        })
        .with_collect_timeout(Duration::from_secs(5))
        .with_poll_interval(Duration::from_millis(100))
        .with_async_assert(|ctx| async move {
            let payloads = ctx
                .received_events
                .into_iter()
                .map(|event| String::from_utf8(event.payload.into_bytes().to_vec()))
                .collect::<std::result::Result<Vec<_>, _>>()?;

            anyhow::ensure!(
                payloads.len() >= 3,
                "预期至少收到 3 条消息，实际收到 {} 条",
                payloads.len()
            );
            anyhow::ensure!(
                payloads.iter().any(|payload| payload.contains("\"seq\":1")),
                "未收到 seq=1 的消息"
            );
            anyhow::ensure!(
                payloads.iter().any(|payload| payload.contains("\"seq\":2")),
                "未收到 seq=2 的消息"
            );
            anyhow::ensure!(
                payloads.iter().any(|payload| payload.contains("\"seq\":3")),
                "未收到 seq=3 的消息"
            );
            Ok(())
        });

    let runtime = SourceIntegrationRuntime::new(docker_tool, vec![source_info]);
    runtime.run(true).await
}
