use anyhow::Result;
use wp_connectors::doris::DorisSinkFactory;

use crate::common::{
    component_tools::DockerComposeTool,
    sink::{
        performance_runtime::{SinkPerformanceConfig, SinkPerformanceRuntime},
        sink_info::SinkInfo,
    },
};
use crate::doris_common::{
    create_doris_test_config, init_doris_database, query_table_count, wait_for_doris_sink_ready,
};

#[tokio::test]
#[ignore = "性能测试默认不在 CI 中运行，请手动执行"]
//执行命令:cargo test --release --package wp-connectors --test doris_tests performance_tests::test_doris_sink_performance --features doris -- --exact --nocapture --ignored
async fn test_doris_sink_performance() -> Result<()> {
    let docker_tool = DockerComposeTool::new("tests/doris/performance_tests.yml")?;
    // 添加sink信息、包括sink工厂、测试的初始化方法，基础方法
    let sink_info = SinkInfo::new(
        DorisSinkFactory,
        create_doris_test_config(),
        |_params| async { query_table_count().await },
    )
    .with_test_name("baseline")
    .with_async_init(|| async { init_doris_database().await })
    .with_async_wait_ready(|_params| async { wait_for_doris_sink_ready().await });
    //性能测试配置
    let config = SinkPerformanceConfig::default()
        //批量大小
        .with_batch_size(1_0000)
        //并行度
        .with_task_count(4);
    let runtime = SinkPerformanceRuntime::new(docker_tool, vec![sink_info], config);
    runtime.run().await
}
