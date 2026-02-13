use serde::{Deserialize, Serialize};
use std::collections::HashMap;

const DEFAULT_TIMEOUT_SECS: u64 = 60;
const DEFAULT_MAX_RETRIES: i32 = 3;

/// Configuration for building a [`DorisSink`](crate::doris::DorisSink) using Stream Load API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DorisSinkConfig {
    /// HTTP endpoint for Doris BE node (e.g., "http://localhost:8040")
    pub endpoint: String,
    /// Target database name
    pub database: String,
    /// Target table name
    pub table: String,
    /// Username for authentication
    pub user: String,
    /// Password for authentication
    pub password: String,
    /// Request timeout in seconds
    pub timeout_secs: u64,
    /// Maximum retry attempts (-1 for unlimited)
    pub max_retries: i32,
    /// Optional custom headers for Stream Load parameters
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<HashMap<String, String>>,
}

impl DorisSinkConfig {
    /// Build configuration with provided parameters.
    ///
    /// # Arguments
    /// * `endpoint` - HTTP endpoint for Doris BE node
    /// * `database` - Target database name
    /// * `table` - Target table name
    /// * `user` - Username for authentication
    /// * `password` - Password for authentication
    /// * `timeout_secs` - Optional request timeout (default: 60s)
    /// * `max_retries` - Optional max retry attempts (default: 3)
    /// * `headers` - Optional custom Stream Load headers
    ///
    /// # Returns
    /// A configured [`DorisSinkConfig`] with defaults applied.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        endpoint: String,
        database: String,
        table: String,
        user: String,
        password: String,
        timeout_secs: Option<u64>,
        max_retries: Option<i32>,
        headers: Option<HashMap<String, String>>,
    ) -> Self {
        Self {
            endpoint: endpoint.trim().to_string(),
            database: database.trim().to_string(),
            table: table.trim().to_string(),
            user: user.trim().to_string(),
            password,
            timeout_secs: timeout_secs.unwrap_or(Self::default_timeout_secs()),
            max_retries: max_retries.unwrap_or(Self::default_max_retries()),
            headers,
        }
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
        let cfg = DorisSinkConfig::new(
            "http://localhost:8040".into(),
            "demo".into(),
            "events".into(),
            "root".into(),
            "".into(),
            None,
            None,
            None,
        );
        assert_eq!(cfg.timeout_secs, DorisSinkConfig::default_timeout_secs());
        assert_eq!(cfg.max_retries, DorisSinkConfig::default_max_retries());
        assert_eq!(cfg.table, "events");
        assert_eq!(cfg.database, "demo");
        assert_eq!(cfg.endpoint, "http://localhost:8040");
        assert_eq!(cfg.headers, None);
    }

    #[test]
    fn config_with_custom_values() {
        let mut headers = HashMap::new();
        headers.insert("max_filter_ratio".into(), "0.1".into());

        let cfg = DorisSinkConfig::new(
            "http://localhost:8040".into(),
            "demo".into(),
            "events".into(),
            "root".into(),
            "pwd".into(),
            Some(120),
            Some(5),
            Some(headers.clone()),
        );
        assert_eq!(cfg.timeout_secs, 120);
        assert_eq!(cfg.max_retries, 5);
        assert_eq!(cfg.headers, Some(headers));
    }
}
