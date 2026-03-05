use serde::{Deserialize, Serialize};

const DEFAULT_TIMEOUT_SECS: u64 = 30;
const DEFAULT_MAX_RETRIES: i32 = 3;
const DEFAULT_PORT: u16 = 8123;

/// ClickHouse Sink 的配置结构，使用 clickhouse 库进行批量写入
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClickHouseSinkConfig {
    /// ClickHouse 主机地址（例如："localhost" 或 "ch.example.com"）
    pub host: String,
    /// ClickHouse 端口号（默认 9000 for native protocol）
    pub port: u16,
    /// 目标数据库名称
    pub database: String,
    /// 目标表名称
    pub table: String,
    /// 认证的用户名
    pub username: String,
    /// 认证的密码
    pub password: String,
    /// 请求超时时间（秒）
    pub timeout_secs: u64,
    /// 最大重试次数（-1 表示无限重试）
    pub max_retries: i32,
}

impl ClickHouseSinkConfig {
    /// 构建配置，应用默认值
    ///
    /// # Arguments
    /// * `host` - ClickHouse 主机地址
    /// * `port` - ClickHouse 端口号（默认：9000）
    /// * `database` - 目标数据库名称
    /// * `table` - 目标表名称
    /// * `username` - 认证用户名
    /// * `password` - 认证密码
    /// * `timeout_secs` - 可选的请求超时时间（默认：30秒）
    /// * `max_retries` - 可选的最大重试次数（默认：3次）
    ///
    /// # Returns
    /// 应用了默认值的 [`ClickHouseSinkConfig`]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        host: String,
        port: Option<u16>,
        database: String,
        table: String,
        username: String,
        password: String,
        timeout_secs: Option<u64>,
        max_retries: Option<i32>,
    ) -> Self {
        Self {
            host: host.trim().to_string(),
            port: port.unwrap_or(Self::default_port()),
            database: database.trim().to_string(),
            table: table.trim().to_string(),
            username: username.trim().to_string(),
            password,
            timeout_secs: timeout_secs.unwrap_or(Self::default_timeout_secs()),
            max_retries: max_retries.unwrap_or(Self::default_max_retries()),
        }
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
    fn test_default_values() {
        let config = ClickHouseSinkConfig::new(
            "localhost".to_string(),
            None,
            "test_db".to_string(),
            "test_table".to_string(),
            "user".to_string(),
            "pass".to_string(),
            None,
            None,
        );

        assert_eq!(config.port, 8123);
        assert_eq!(config.timeout_secs, 30);
        assert_eq!(config.max_retries, 3);
    }

    #[test]
    fn test_parameter_trimming() {
        let config = ClickHouseSinkConfig::new(
            "  localhost  ".to_string(),
            None,
            "  test_db  ".to_string(),
            "  test_table  ".to_string(),
            "  user  ".to_string(),
            "pass".to_string(),
            None,
            None,
        );

        assert_eq!(config.host, "localhost");
        assert_eq!(config.database, "test_db");
        assert_eq!(config.table, "test_table");
        assert_eq!(config.username, "user");
    }

    #[test]
    fn test_custom_values() {
        let config = ClickHouseSinkConfig::new(
            "ch.example.com".to_string(),
            Some(9440),
            "production_db".to_string(),
            "events".to_string(),
            "admin".to_string(),
            "secret".to_string(),
            Some(60),
            Some(5),
        );

        assert_eq!(config.host, "ch.example.com");
        assert_eq!(config.port, 9440);
        assert_eq!(config.database, "production_db");
        assert_eq!(config.table, "events");
        assert_eq!(config.username, "admin");
        assert_eq!(config.password, "secret");
        assert_eq!(config.timeout_secs, 60);
        assert_eq!(config.max_retries, 5);
    }

    #[test]
    fn test_default_methods() {
        assert_eq!(ClickHouseSinkConfig::default_port(), 8123);
        assert_eq!(ClickHouseSinkConfig::default_timeout_secs(), 30);
        assert_eq!(ClickHouseSinkConfig::default_max_retries(), 3);
    }

    #[test]
    fn test_password_not_trimmed() {
        let config = ClickHouseSinkConfig::new(
            "localhost".to_string(),
            None,
            "test_db".to_string(),
            "test_table".to_string(),
            "user".to_string(),
            "  pass with spaces  ".to_string(),
            None,
            None,
        );

        // Password should not be trimmed
        assert_eq!(config.password, "  pass with spaces  ");
    }
}
