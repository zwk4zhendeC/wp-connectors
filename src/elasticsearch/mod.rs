//! Elasticsearch sink implementation for wp-connectors
//!
//! 提供 Sink 实现，负责连接 Elasticsearch 并使用 Bulk API 批量写入数据。
//!
//! # 功能特性
//!
//! - 使用 Elasticsearch Bulk API 进行高性能批量写入
//! - 支持 HTTP Basic 认证
//! - 自动重试机制（指数退避）
//! - NDJSON 格式支持
//! - 性能监控和统计
//!
//! # 使用示例
//!
//! ```rust,no_run
//! use wp_connectors::elasticsearch::{ElasticsearchSink, ElasticsearchSinkConfig};
//! use wp_connector_api::AsyncRecordSink;
//! use wp_model_core::model::{DataRecord, DataField};
//! use std::sync::Arc;
//!
//! # async fn example() -> anyhow::Result<()> {
//! // 创建配置
//! let config = ElasticsearchSinkConfig::new(
//!     Some("http".to_string()),      // 协议（可选，默认 http）
//!     "localhost".to_string(),        // 主机地址
//!     Some(9200),                     // 端口（可选，默认 9200）
//!     "my_index".to_string(),         // 索引名称
//!     "elastic".to_string(),          // 用户名
//!     "password".to_string(),         // 密码
//!     Some(60),                       // 超时时间（秒）
//!     Some(3),                        // 最大重试次数
//! );
//!
//! // 创建 sink
//! let mut sink = ElasticsearchSink::new(config).await?;
//!
//! // 创建数据记录
//! let mut record = DataRecord::default();
//! record.append(DataField::from_digit("id", 1));
//! record.append(DataField::from_chars("message", "test message"));
//!
//! // 写入数据
//! sink.sink_records(vec![Arc::new(record)]).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # 配置参数
//!
//! - `protocol`: 协议（http 或 https），默认 http
//! - `host`: Elasticsearch 主机地址（必填）
//! - `port`: Elasticsearch 端口号，默认 9200
//! - `index`: 目标索引名称（必填）
//! - `username`: 认证用户名（必填）
//! - `password`: 认证密码（可选）
//! - `timeout_secs`: 请求超时时间，默认 60 秒
//! - `max_retries`: 最大重试次数，默认 3 次，-1 表示无限重试
//!
//! # 错误处理
//!
//! - 4xx 客户端错误：不重试，立即返回错误
//! - 5xx 服务器错误：使用指数退避重试
//! - 网络错误：使用指数退避重试
//! - 超时错误：使用指数退避重试
//!
//! # 重试策略
//!
//! 重试使用指数退避算法：
//! - 延迟时间 = 1000ms * 2^重试次数
//! - 最大延迟上限为 2^10 = 1024 秒
//!
//! # 性能优化
//!
//! - 批量大小由外部调用者控制
//! - HTTP 连接复用
//! - 禁用代理以减少延迟
//! - 使用 TimeStatUtils 跟踪性能指标

mod config;
mod factory;
mod sink;

pub use config::ElasticsearchSinkConfig;
pub use factory::ElasticsearchSinkFactory;
pub use sink::ElasticsearchSink;
