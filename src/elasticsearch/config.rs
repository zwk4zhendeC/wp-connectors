use serde::{Deserialize, Serialize};

const DEFAULT_TIMEOUT_SECS: u64 = 60;
const DEFAULT_MAX_RETRIES: i32 = 3;
const DEFAULT_PROTOCOL: &str = "http";
const DEFAULT_PORT: u16 = 9200;

/// Elasticsearch Sink 的配置结构，使用 Bulk API 进行批量写入
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ElasticsearchSinkConfig {
    /// 协议（http 或 https）
    pub protocol: String,
    /// Elasticsearch 主机地址（例如："localhost" 或 "es.example.com"）
    pub host: String,
    /// Elasticsearch 端口号
    pub port: u16,
    /// 目标索引名称
    pub index: String,
    /// HTTP Basic 认证的用户名
    pub username: String,
    /// HTTP Basic 认证的密码
    pub password: String,
    /// 请求超时时间（秒）
    pub timeout_secs: u64,
    /// 最大重试次数（-1 表示无限重试）
    pub max_retries: i32,
}

impl ElasticsearchSinkConfig {
    /// 构建配置，应用默认值
    ///
    /// # Arguments
    /// * `protocol` - 协议（http 或 https，默认：http）
    /// * `host` - Elasticsearch 主机地址
    /// * `port` - Elasticsearch 端口号（默认：9200）
    /// * `index` - 目标索引名称
    /// * `username` - 认证用户名
    /// * `password` - 认证密码
    /// * `timeout_secs` - 可选的请求超时时间（默认：60秒）
    /// * `max_retries` - 可选的最大重试次数（默认：3次）
    ///
    /// # Returns
    /// 应用了默认值的 [`ElasticsearchSinkConfig`]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        protocol: Option<String>,
        host: String,
        port: Option<u16>,
        index: String,
        username: String,
        password: String,
        timeout_secs: Option<u64>,
        max_retries: Option<i32>,
    ) -> Self {
        Self {
            protocol: protocol
                .unwrap_or_else(|| Self::default_protocol().to_string())
                .trim()
                .to_lowercase(),
            host: host.trim().to_string(),
            port: port.unwrap_or(Self::default_port()),
            index: index.trim().to_string(),
            username: username.trim().to_string(),
            password,
            timeout_secs: timeout_secs.unwrap_or(Self::default_timeout_secs()),
            max_retries: max_retries.unwrap_or(Self::default_max_retries()),
        }
    }

    /// 获取完整的 endpoint URL
    pub fn endpoint(&self) -> String {
        format!("{}://{}:{}", self.protocol, self.host, self.port)
    }

    pub fn default_protocol() -> &'static str {
        DEFAULT_PROTOCOL
    }

    pub fn default_port() -> u16 {
        DEFAULT_PORT
    }

    pub fn default_timeout_secs() -> u64 {
        DEFAULT_TIMEOUT_SECS
    }

    pub fn default_max_retries() -> i32 {
        DEFAULT_MAX_RETRIES
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults() {
        let cfg = ElasticsearchSinkConfig::new(
            None,
            "localhost".into(),
            None,
            "test_index".into(),
            "elastic".into(),
            "password".into(),
            None,
            None,
        );
        assert_eq!(cfg.protocol, "http");
        assert_eq!(cfg.host, "localhost");
        assert_eq!(cfg.port, 9200);
        assert_eq!(
            cfg.timeout_secs,
            ElasticsearchSinkConfig::default_timeout_secs()
        );
        assert_eq!(
            cfg.max_retries,
            ElasticsearchSinkConfig::default_max_retries()
        );
        assert_eq!(cfg.index, "test_index");
        assert_eq!(cfg.username, "elastic");
        assert_eq!(cfg.endpoint(), "http://localhost:9200");
    }

    #[test]
    fn config_with_custom_values() {
        let cfg = ElasticsearchSinkConfig::new(
            Some("https".into()),
            "es.example.com".into(),
            Some(9243),
            "test_index".into(),
            "elastic".into(),
            "password".into(),
            Some(120),
            Some(5),
        );
        assert_eq!(cfg.protocol, "https");
        assert_eq!(cfg.host, "es.example.com");
        assert_eq!(cfg.port, 9243);
        assert_eq!(cfg.timeout_secs, 120);
        assert_eq!(cfg.max_retries, 5);
        assert_eq!(cfg.endpoint(), "https://es.example.com:9243");
    }

    #[test]
    fn config_trims_whitespace() {
        let cfg = ElasticsearchSinkConfig::new(
            Some("  HTTP  ".into()),
            "  localhost  ".into(),
            None,
            "  test_index  ".into(),
            "  elastic  ".into(),
            "password".into(),
            None,
            None,
        );
        assert_eq!(cfg.protocol, "http");
        assert_eq!(cfg.host, "localhost");
        assert_eq!(cfg.index, "test_index");
        assert_eq!(cfg.username, "elastic");
    }

    #[test]
    fn config_endpoint_method() {
        let cfg = ElasticsearchSinkConfig::new(
            Some("https".into()),
            "192.168.1.100".into(),
            Some(9200),
            "logs".into(),
            "user".into(),
            "pass".into(),
            None,
            None,
        );
        assert_eq!(cfg.endpoint(), "https://192.168.1.100:9200");
    }
}
