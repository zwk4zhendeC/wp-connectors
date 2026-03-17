#![allow(dead_code)]

use super::sink_info::SinkInfo;
use crate::common::component_tools::ComponentTool;
use anyhow::Result;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};
use wp_connector_api::{SinkBuildCtx, SinkFactory, SinkSpec};

static NEXT_TEST_RECORD_ID: AtomicI64 = AtomicI64::new(1);

/// Sink 集成测试运行时
pub struct SinkIntegrationRuntime<T: ComponentTool, F: SinkFactory> {
    /// 组件工具
    component_tool: T,
    /// Sink 集成测试信息列表
    sink_infos: Vec<SinkInfo<F>>,
}

impl<T: ComponentTool + Sync, F: SinkFactory> SinkIntegrationRuntime<T, F> {
    /// 创建新的运行时实例
    pub fn new(component_tool: T, sink_infos: Vec<SinkInfo<F>>) -> Self {
        Self {
            component_tool,
            sink_infos,
        }
    }

    /// 运行集成测试
    pub async fn run(&self) -> Result<()> {
        println!("启动组件...");
        self.component_tool.setup_and_up().await?;

        // 2. 遍历每个 SinkInfo
        for (idx, sink_info) in self.sink_infos.iter().enumerate() {
            let kind = sink_info.factory().kind();
            let display_name = format_display_name(kind, sink_info.test_name(), idx);
            println!("\n========== 测试 Sink: {} =========", display_name);

            // 2.1 执行异步初始化
            println!("执行初始化...");
            sink_info.init().await?;
            sink_info.wait_ready().await?;

            // 2.2 构建 SinkSpec
            let spec = SinkSpec {
                group: "integration_test".to_string(),
                name: display_name.clone(),
                kind: kind.to_string(),
                connector_id: display_name.clone(),
                params: sink_info.params().clone(),
                filter: None,
            };

            println!("Sink 规格: {} ({})", spec.name, spec.kind);

            // 2.3 创建 Sink
            println!("创建 Sink...");
            let ctx = SinkBuildCtx::new(PathBuf::from("."));
            let mut sink = sink_info.factory().build(&spec, &ctx).await?;

            // 2.4 获取发送前的数量
            let count_before = sink_info.count().await?;
            println!("发送前数量: {}", count_before);

            // 2.5 创建测试数据（3条）
            let test_records = self.create_test_records(3);
            println!("创建了 {} 条测试记录", test_records.len());

            // 2.6 发送数据
            println!("发送数据...");
            for record in &test_records {
                sink.sink.sink_record(record).await?;
            }

            // 等待数据写入
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

            // 2.7 检查发送后的数量
            let count_after = sink_info.count().await?;
            println!("发送后数量: {}", count_after);

            let diff = count_after - count_before;
            if diff == 3 {
                println!("✅ 数据发送成功，新增 {} 条记录", diff);
            } else {
                anyhow::bail!("❌ 数据发送失败，预期新增 3 条，实际新增 {} 条", diff);
            }

            // 2.8 重启 ComponentTool
            println!("\n重启 外部组件...");
            self.component_tool.restart().await?;
            self.component_tool.wait_started().await?;
            sink_info.wait_ready().await?;

            // 2.9 重启后再次发送
            println!("重启后再次发送数据...");
            let count_before_restart = sink_info.count().await?;
            println!("重启后发送前数量: {}", count_before_restart);

            // 重新创建 Sink（因为连接可能已断开）
            let ctx = SinkBuildCtx::new(PathBuf::from("."));
            let mut sink = sink_info.factory().build(&spec, &ctx).await?;

            let retry_test_records = self.create_test_records(3);
            println!("重启后创建了 {} 条测试记录", retry_test_records.len());

            for record in &retry_test_records {
                sink.sink.sink_record(record).await?;
            }

            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

            let count_after_restart = sink_info.count().await?;
            println!("重启后发送后数量: {}", count_after_restart);

            let diff_restart = count_after_restart - count_before_restart;
            if diff_restart >= 3 {
                println!("✅ 重启后数据发送成功，新增 {} 条记录", diff_restart);
            } else {
                anyhow::bail!(
                    "❌ 重启后数据发送失败，预期新增 3 条，实际新增 {} 条",
                    diff_restart
                );
            }
        }

        // 3. 清理
        println!("\n清理环境...");
        self.component_tool.down().await?;

        Ok(())
    }

    /// 创建测试记录（参考 examples/common_utils.rs）
    fn create_test_records(&self, count: usize) -> Vec<wp_model_core::model::DataRecord> {
        use wp_model_core::model::{DataField, DataRecord};

        let start_id = NEXT_TEST_RECORD_ID.fetch_add(count as i64, Ordering::SeqCst);

        (0..count)
            .map(|i| {
                let id = start_id + i as i64;
                let mut record = DataRecord::default();
                record.append(DataField::from_digit("wp_event_id", id));
                record.append(DataField::from_chars(
                    "wp_src_key",
                    format!("integration_test_{}", id),
                ));
                record.append(DataField::from_chars("sip", "192.168.1.100"));
                record.append(DataField::from_chars(
                    "timestamp",
                    chrono::Utc::now().format("%Y-%m-%d %H:%M:%S").to_string(),
                ));
                record.append(DataField::from_chars(
                    "http/request",
                    format!("GET /api/test/{} HTTP/1.1", id),
                ));
                record.append(DataField::from_digit("status", 200));
                record.append(DataField::from_digit("size", 1024 + i as i64));
                record.append(DataField::from_chars("referer", format!("{:06}", id)));
                record.append(DataField::from_chars(
                    "http/agent",
                    "Mozilla/5.0 (Integration Test)",
                ));
                record
            })
            .collect()
    }
}

fn format_display_name(kind: &str, test_name: Option<&str>, idx: usize) -> String {
    match test_name {
        Some(name) if !name.trim().is_empty() => format!("{}_{}_{}", kind, name.trim(), idx + 1),
        _ => format!("{}_{}", kind, idx + 1),
    }
}
