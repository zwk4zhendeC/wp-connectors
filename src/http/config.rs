use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use wp_model_core::model::fmt_def::TextFmt;

const DEFAULT_METHOD: &str = "POST";
const DEFAULT_CONTENT_TYPE: &str = "application/x-ndjson";
const DEFAULT_TIMEOUT_SECS: u64 = 60;
const DEFAULT_MAX_RETRIES: i32 = 3;
const DEFAULT_FMT: TextFmt = TextFmt::Json;

/// HTTP Sink 的配置结构。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HTTPSinkConfig {
    /// HTTP endpoint（例如: http://localhost:8080/ingest）
    pub endpoint: String,
    /// HTTP 方法（POST/PUT/PATCH）
    pub method: String,
    /// 记录格式
    pub fmt: TextFmt,
    /// 请求 Content-Type
    pub content_type: String,
    /// HTTP Basic 认证用户名（可选）
    pub username: String,
    /// HTTP Basic 认证密码（可选）
    pub password: String,
    /// 请求超时时间（秒）
    pub timeout_secs: u64,
    /// 最大重试次数（-1 表示无限重试）
    pub max_retries: i32,
    /// 额外请求头
    pub headers: HashMap<String, String>,
}

impl HTTPSinkConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        endpoint: String,
        method: Option<String>,
        fmt: Option<TextFmt>,
        content_type: Option<String>,
        username: Option<String>,
        password: Option<String>,
        timeout_secs: Option<u64>,
        max_retries: Option<i32>,
        headers: Option<HashMap<String, String>>,
    ) -> Self {
        let normalized_method = method
            .unwrap_or_else(|| Self::default_method().to_string())
            .trim()
            .to_ascii_uppercase();

        Self {
            endpoint: endpoint.trim().to_string(),
            method: normalized_method,
            fmt: fmt.unwrap_or(Self::default_fmt()),
            content_type: content_type
                .unwrap_or_else(|| Self::default_content_type().to_string())
                .trim()
                .to_string(),
            username: username.unwrap_or_default().trim().to_string(),
            password: password.unwrap_or_default(),
            timeout_secs: timeout_secs.unwrap_or(Self::default_timeout_secs()),
            max_retries: max_retries.unwrap_or(Self::default_max_retries()),
            headers: headers.unwrap_or_default(),
        }
    }

    pub const fn default_method() -> &'static str {
        DEFAULT_METHOD
    }

    pub const fn default_content_type() -> &'static str {
        DEFAULT_CONTENT_TYPE
    }

    pub const fn default_timeout_secs() -> u64 {
        DEFAULT_TIMEOUT_SECS
    }

    pub const fn default_max_retries() -> i32 {
        DEFAULT_MAX_RETRIES
    }

    pub const fn default_fmt() -> TextFmt {
        DEFAULT_FMT
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults() {
        let cfg = HTTPSinkConfig::new(
            "http://localhost:8080/ingest".into(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );

        assert_eq!(cfg.method, HTTPSinkConfig::default_method());
        assert_eq!(cfg.fmt, HTTPSinkConfig::default_fmt());
        assert_eq!(cfg.content_type, HTTPSinkConfig::default_content_type());
        assert_eq!(cfg.timeout_secs, HTTPSinkConfig::default_timeout_secs());
        assert_eq!(cfg.max_retries, HTTPSinkConfig::default_max_retries());
        assert_eq!(cfg.username, "");
        assert!(cfg.headers.is_empty());
    }

    #[test]
    fn config_normalizes_input() {
        let cfg = HTTPSinkConfig::new(
            "  http://localhost:8080/ingest  ".into(),
            Some("  patch ".into()),
            Some(TextFmt::Kv),
            Some(" application/json ".into()),
            Some("  admin  ".into()),
            Some("secret".into()),
            Some(120),
            Some(5),
            None,
        );

        assert_eq!(cfg.endpoint, "http://localhost:8080/ingest");
        assert_eq!(cfg.method, "PATCH");
        assert_eq!(cfg.fmt, TextFmt::Kv);
        assert_eq!(cfg.content_type, "application/json");
        assert_eq!(cfg.username, "admin");
        assert_eq!(cfg.password, "secret");
        assert_eq!(cfg.timeout_secs, 120);
        assert_eq!(cfg.max_retries, 5);
    }
}
