//! ClickHouse sink implementation for wp-connectors
//!
//! 提供 Sink 实现，负责连接 ClickHouse 并使用 clickhouse 库批量写入数据。
//!
//! # 功能特性
//!
//! - 使用 clickhouse 库进行高性能批量写入
//! - 支持认证
//! - 自动重试机制（指数退避）
//! - JSONEachRow 格式支持
//! - 性能监控和统计
//!
//! # 使用示例
//!
//! ```rust,no_run
//! use wp_connectors::clickhouse::{ClickHouseSink, ClickHouseSinkConfig};
//! use wp_connector_api::AsyncRecordSink;
//! use wp_model_core::model::{DataRecord, DataField};
//! use std::sync::Arc;
//!
//! # async fn example() -> anyhow::Result<()> {
//! // 创建配置
//! let config = ClickHouseSinkConfig::new(
//!     "localhost".to_string(),        // 主机地址
//!     Some(8123),                     // 端口（可选，默认 8123，HTTP 接口）
//!     "default".to_string(),          // 数据库名称
//!     "wp_nginx".to_string(),         // 表名称
//!     "default".to_string(),          // 用户名
//!     "".to_string(),                 // 密码
//!     Some(30),                       // 超时时间（秒）
//!     Some(3),                        // 最大重试次数
//! );
//!
//! // 创建 sink
//! let mut sink = ClickHouseSink::new(config).await?;
//!
//! // 创建数据记录
//! let mut record = DataRecord::default();
//! record.append(DataField::from_digit("wp_event_id", 1));
//! record.append(DataField::from_chars("sip", "192.168.1.1"));
//!
//! // 写入数据
//! sink.sink_records(vec![Arc::new(record)]).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # 配置参数
//!
//! - `host`: ClickHouse 主机地址（必填）
//! - `port`: ClickHouse HTTP 端口号，默认 8123（clickhouse 库使用 HTTP 协议）
//! - `database`: 目标数据库名称（必填）
//! - `table`: 目标表名称（必填）
//! - `username`: 认证用户名（必填）
//! - `password`: 认证密码（可选）
//! - `timeout_secs`: 请求超时时间，默认 30 秒
//! - `max_retries`: 最大重试次数，默认 3 次，-1 表示无限重试
//!
//! # 错误处理
//!
//! - 4xx 客户端错误：不重试，立即返回错误
//! - 5xx 服务器错误：使用指数退避重试
//! - 网络错误：使用指数退避重试
//!
//! # 重试策略
//!
//! 重试使用指数退避算法：
//! - 初始延迟：1 秒
//! - 延迟时间 = 1000ms * 2^重试次数
//! - 最大延迟上限为 1024 秒
//!
//! # 性能优化
//!
//! - 批量大小由外部调用者控制
//! - 连接复用
//! - 使用 TimeStatUtils 跟踪性能指标

mod config;
mod factory;
mod sink;

pub use config::ClickHouseSinkConfig;
pub use factory::ClickHouseSinkFactory;
pub use sink::ClickHouseSink;
