/// HTTP Sink configuration structure
///
/// This module defines the configuration parameters for the HTTP Sink.
use std::collections::HashMap;

/// Configuration for HTTP Sink
///
/// Contains all parameters needed to configure HTTP data transmission,
/// including connection settings, authentication, data formatting, and retry behavior.
#[derive(Debug, Clone)]
pub struct HttpSinkConfig {
    /// Target HTTP(S) endpoint URL
    pub endpoint: String,

    /// HTTP method (GET, POST, PUT, PATCH, DELETE)
    pub method: String,

    /// Optional username for HTTP Basic Authentication
    pub username: String,

    /// Optional password for HTTP Basic Authentication
    pub password: String,

    /// Custom HTTP headers (including Content-Type if specified)
    pub headers: HashMap<String, String>,

    /// Output format (json, ndjson, csv, kv, raw, proto-text)
    pub fmt: String,

    /// Batch size for bulk sending (default: 1)
    pub batch_size: usize,

    /// Request timeout in seconds (default: 60)
    pub timeout_secs: u64,

    /// Maximum retry attempts (-1 for infinite, 0 for no retry, default: 3)
    pub max_retries: i32,

    /// Compression algorithm (none, gzip)
    pub compression: String,
}

impl HttpSinkConfig {
    /// Create a new HTTP Sink configuration with optional parameters
    ///
    /// # Arguments
    ///
    /// * `endpoint` - Target HTTP(S) URL (required)
    /// * `method` - HTTP method (optional, defaults to POST)
    /// * `username` - Basic Auth username (optional)
    /// * `password` - Basic Auth password (optional)
    /// * `headers` - Custom HTTP headers (optional)
    /// * `fmt` - Output format (optional, defaults to json)
    /// * `batch_size` - Batch size (optional, defaults to 1)
    /// * `timeout_secs` - Timeout in seconds (optional, defaults to 60)
    /// * `max_retries` - Max retry attempts (optional, defaults to 3)
    /// * `compression` - Compression algorithm (optional, defaults to none)
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        endpoint: String,
        method: Option<String>,
        username: Option<String>,
        password: Option<String>,
        headers: Option<HashMap<String, String>>,
        fmt: Option<String>,
        batch_size: Option<usize>,
        timeout_secs: Option<u64>,
        max_retries: Option<i32>,
        compression: Option<String>,
    ) -> Self {
        Self {
            endpoint,
            method: method.unwrap_or_else(Self::default_method),
            username: username.unwrap_or_default(),
            password: password.unwrap_or_default(),
            headers: headers.unwrap_or_default(),
            fmt: fmt.unwrap_or_else(Self::default_fmt),
            batch_size: batch_size.unwrap_or_else(Self::default_batch_size),
            timeout_secs: timeout_secs.unwrap_or_else(Self::default_timeout_secs),
            max_retries: max_retries.unwrap_or_else(Self::default_max_retries),
            compression: compression.unwrap_or_else(Self::default_compression),
        }
    }

    /// Default HTTP method
    pub fn default_method() -> String {
        "POST".to_string()
    }

    /// Default output format
    pub fn default_fmt() -> String {
        "json".to_string()
    }

    /// Default batch size
    pub fn default_batch_size() -> usize {
        1
    }

    /// Default timeout in seconds
    pub fn default_timeout_secs() -> u64 {
        60
    }

    /// Default maximum retry attempts
    pub fn default_max_retries() -> i32 {
        3
    }

    /// Default compression algorithm
    pub fn default_compression() -> String {
        "none".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults() {
        let cfg = HttpSinkConfig::new(
            "http://example.com".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );

        assert_eq!(cfg.endpoint, "http://example.com");
        assert_eq!(cfg.method, "POST");
        assert_eq!(cfg.username, "");
        assert_eq!(cfg.password, "");
        assert!(cfg.headers.is_empty());
        assert_eq!(cfg.fmt, "json");
        assert_eq!(cfg.batch_size, 1);
        assert_eq!(cfg.timeout_secs, 60);
        assert_eq!(cfg.max_retries, 3);
        assert_eq!(cfg.compression, "none");
    }

    #[test]
    fn config_with_custom_values() {
        let mut headers = HashMap::new();
        headers.insert("X-Custom-Header".to_string(), "value".to_string());

        let cfg = HttpSinkConfig::new(
            "https://api.example.com/webhook".to_string(),
            Some("PUT".to_string()),
            Some("user".to_string()),
            Some("pass".to_string()),
            Some(headers.clone()),
            Some("ndjson".to_string()),
            Some(100),
            Some(30),
            Some(5),
            Some("gzip".to_string()),
        );

        assert_eq!(cfg.endpoint, "https://api.example.com/webhook");
        assert_eq!(cfg.method, "PUT");
        assert_eq!(cfg.username, "user");
        assert_eq!(cfg.password, "pass");
        assert_eq!(
            cfg.headers.get("X-Custom-Header"),
            Some(&"value".to_string())
        );
        assert_eq!(cfg.fmt, "ndjson");
        assert_eq!(cfg.batch_size, 100);
        assert_eq!(cfg.timeout_secs, 30);
        assert_eq!(cfg.max_retries, 5);
        assert_eq!(cfg.compression, "gzip");
    }

    #[test]
    fn config_endpoint_parsing() {
        let cfg = HttpSinkConfig::new(
            "https://api.example.com:8443/webhook".to_string(),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );

        assert_eq!(cfg.endpoint, "https://api.example.com:8443/webhook");
    }
}
