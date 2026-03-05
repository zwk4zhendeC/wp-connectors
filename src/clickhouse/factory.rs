use crate::clickhouse::{ClickHouseSink, ClickHouseSinkConfig};
use async_trait::async_trait;
use serde_json::{Value, json};
use wp_connector_api::{
    ConnectorDef, ConnectorScope, ParamMap, SinkBuildCtx, SinkDefProvider, SinkError, SinkFactory,
    SinkHandle, SinkReason, SinkResult, SinkSpec,
};

/// ClickHouse Sink 工厂，负责验证配置和构建 Sink 实例
pub struct ClickHouseSinkFactory;

#[async_trait]
impl SinkFactory for ClickHouseSinkFactory {
    fn kind(&self) -> &'static str {
        "clickhouse"
    }

    fn validate_spec(&self, spec: &SinkSpec) -> SinkResult<()> {
        // 验证必填参数非空
        ensure_not_empty(spec, "host")?;
        ensure_not_empty(spec, "database")?;
        ensure_not_empty(spec, "table")?;
        ensure_not_empty(spec, "username")?;

        // 验证 port（如果提供）
        if let Some(port) = get_u64(spec, "port")
            && (port == 0 || port > 65535)
        {
            return Err(SinkReason::sink("clickhouse.port must be between 1 and 65535").into());
        }

        // 验证 timeout_secs
        if let Some(timeout) = get_u64(spec, "timeout_secs")
            && timeout == 0
        {
            return Err(SinkReason::sink("clickhouse.timeout_secs must be > 0").into());
        }

        // 验证 max_retries
        if let Some(retries) = get_i64(spec, "max_retries")
            && retries < -1
        {
            return Err(SinkReason::sink("clickhouse.max_retries must be >= -1").into());
        }

        Ok(())
    }

    async fn build(&self, spec: &SinkSpec, _ctx: &SinkBuildCtx) -> SinkResult<SinkHandle> {
        let host = required_param(spec, "host")?;
        let port = get_u64(spec, "port").map(|p| p as u16);
        let database = required_param(spec, "database")?;
        let table = required_param(spec, "table")?;
        let username = required_param(spec, "username")?;
        let password = optional_string(spec, "password").unwrap_or_default();
        let timeout_secs = get_u64(spec, "timeout_secs");
        let max_retries = get_i64(spec, "max_retries").map(|r| r as i32);

        let cfg = ClickHouseSinkConfig::new(
            host,
            port,
            database,
            table,
            username,
            password,
            timeout_secs,
            max_retries,
        );

        let sink = ClickHouseSink::new(cfg).await.map_err(|err| {
            SinkError::from(SinkReason::sink(format!(
                "init clickhouse sink failed: {err}"
            )))
        })?;

        Ok(SinkHandle::new(Box::new(sink)))
    }
}

impl SinkDefProvider for ClickHouseSinkFactory {
    fn sink_def(&self) -> ConnectorDef {
        ConnectorDef {
            id: "clickhouse_sink".to_string(),
            kind: self.kind().to_string(),
            scope: ConnectorScope::Sink,
            allow_override: vec![
                "host",
                "port",
                "database",
                "table",
                "username",
                "password",
                "timeout_secs",
                "max_retries",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
            default_params: clickhouse_defaults(),
            origin: Some("wp-connectors:clickhouse_sink".to_string()),
        }
    }
}

/// 保证指定参数存在且非空
fn ensure_not_empty(spec: &SinkSpec, key: &str) -> SinkResult<()> {
    let value = spec
        .params
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string();
    if value.is_empty() {
        return Err(SinkReason::sink(format!("clickhouse.{key} must not be empty")).into());
    }
    Ok(())
}

/// 读取必填参数并返回修剪后的字符串
fn required_param(spec: &SinkSpec, key: &str) -> SinkResult<String> {
    spec.params
        .get(key)
        .and_then(Value::as_str)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| SinkReason::sink(format!("clickhouse.{key} must not be empty")).into())
}

/// 读取可选字符串参数
fn optional_string(spec: &SinkSpec, key: &str) -> Option<String> {
    spec.params
        .get(key)
        .and_then(Value::as_str)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// 将参数解析为 `u64`
fn get_u64(spec: &SinkSpec, key: &str) -> Option<u64> {
    spec.params.get(key).and_then(Value::as_u64)
}

/// 将参数解析为 `i64`
fn get_i64(spec: &SinkSpec, key: &str) -> Option<i64> {
    spec.params.get(key).and_then(Value::as_i64)
}

/// 生成 ClickHouse Sink 的默认参数
fn clickhouse_defaults() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("host".into(), json!("localhost"));
    params.insert("port".into(), json!(ClickHouseSinkConfig::default_port()));
    params.insert("database".into(), json!("default"));
    params.insert("table".into(), json!("wp_logs"));
    params.insert("username".into(), json!("default"));
    params.insert("password".into(), json!(""));
    params.insert(
        "timeout_secs".into(),
        json!(ClickHouseSinkConfig::default_timeout_secs()),
    );
    params.insert(
        "max_retries".into(),
        json!(ClickHouseSinkConfig::default_max_retries()),
    );
    params
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn base_spec() -> SinkSpec {
        let mut params = BTreeMap::new();
        params.insert("host".into(), Value::String("localhost".into()));
        params.insert("database".into(), Value::String("test_db".into()));
        params.insert("table".into(), Value::String("test_table".into()));
        params.insert("username".into(), Value::String("default".into()));
        SinkSpec {
            name: "clickhouse_sink".into(),
            kind: "clickhouse".into(),
            connector_id: String::new(),
            group: "test".into(),
            params,
            filter: None,
        }
    }

    #[test]
    fn test_kind() {
        let factory = ClickHouseSinkFactory;
        assert_eq!(factory.kind(), "clickhouse");
    }

    #[test]
    fn validate_rejects_empty_host() {
        let mut spec = base_spec();
        spec.params.insert("host".into(), Value::String("".into()));
        let factory = ClickHouseSinkFactory;
        assert!(factory.validate_spec(&spec).is_err());
    }

    #[test]
    fn validate_rejects_missing_host() {
        let mut spec = base_spec();
        spec.params.remove("host");
        let factory = ClickHouseSinkFactory;
        assert!(factory.validate_spec(&spec).is_err());
    }

    #[test]
    fn validate_rejects_empty_database() {
        let mut spec = base_spec();
        spec.params
            .insert("database".into(), Value::String("".into()));
        let factory = ClickHouseSinkFactory;
        assert!(factory.validate_spec(&spec).is_err());
    }

    #[test]
    fn validate_rejects_missing_database() {
        let mut spec = base_spec();
        spec.params.remove("database");
        let factory = ClickHouseSinkFactory;
        assert!(factory.validate_spec(&spec).is_err());
    }

    #[test]
    fn validate_rejects_empty_table() {
        let mut spec = base_spec();
        spec.params.insert("table".into(), Value::String("".into()));
        let factory = ClickHouseSinkFactory;
        assert!(factory.validate_spec(&spec).is_err());
    }

    #[test]
    fn validate_rejects_missing_table() {
        let mut spec = base_spec();
        spec.params.remove("table");
        let factory = ClickHouseSinkFactory;
        assert!(factory.validate_spec(&spec).is_err());
    }

    #[test]
    fn validate_rejects_empty_username() {
        let mut spec = base_spec();
        spec.params
            .insert("username".into(), Value::String("".into()));
        let factory = ClickHouseSinkFactory;
        assert!(factory.validate_spec(&spec).is_err());
    }

    #[test]
    fn validate_rejects_missing_username() {
        let mut spec = base_spec();
        spec.params.remove("username");
        let factory = ClickHouseSinkFactory;
        assert!(factory.validate_spec(&spec).is_err());
    }

    #[test]
    fn validate_rejects_zero_port() {
        let mut spec = base_spec();
        spec.params.insert("port".into(), Value::Number(0.into()));
        let factory = ClickHouseSinkFactory;
        assert!(factory.validate_spec(&spec).is_err());
    }

    #[test]
    fn validate_rejects_port_too_large() {
        let mut spec = base_spec();
        spec.params
            .insert("port".into(), Value::Number(65536.into()));
        let factory = ClickHouseSinkFactory;
        assert!(factory.validate_spec(&spec).is_err());
    }

    #[test]
    fn validate_accepts_valid_port() {
        let mut spec = base_spec();
        spec.params
            .insert("port".into(), Value::Number(8123.into()));
        let factory = ClickHouseSinkFactory;
        assert!(factory.validate_spec(&spec).is_ok());
    }

    #[test]
    fn validate_rejects_zero_timeout() {
        let mut spec = base_spec();
        spec.params
            .insert("timeout_secs".into(), Value::Number(0.into()));
        let factory = ClickHouseSinkFactory;
        assert!(factory.validate_spec(&spec).is_err());
    }

    #[test]
    fn validate_accepts_positive_timeout() {
        let mut spec = base_spec();
        spec.params
            .insert("timeout_secs".into(), Value::Number(30.into()));
        let factory = ClickHouseSinkFactory;
        assert!(factory.validate_spec(&spec).is_ok());
    }

    #[test]
    fn validate_rejects_max_retries_less_than_minus_one() {
        let mut spec = base_spec();
        spec.params
            .insert("max_retries".into(), Value::Number((-2).into()));
        let factory = ClickHouseSinkFactory;
        assert!(factory.validate_spec(&spec).is_err());
    }

    #[test]
    fn validate_accepts_max_retries_minus_one() {
        let mut spec = base_spec();
        spec.params
            .insert("max_retries".into(), Value::Number((-1).into()));
        let factory = ClickHouseSinkFactory;
        assert!(factory.validate_spec(&spec).is_ok());
    }

    #[test]
    fn validate_accepts_max_retries_zero() {
        let mut spec = base_spec();
        spec.params
            .insert("max_retries".into(), Value::Number(0.into()));
        let factory = ClickHouseSinkFactory;
        assert!(factory.validate_spec(&spec).is_ok());
    }

    #[test]
    fn validate_accepts_max_retries_positive() {
        let mut spec = base_spec();
        spec.params
            .insert("max_retries".into(), Value::Number(3.into()));
        let factory = ClickHouseSinkFactory;
        assert!(factory.validate_spec(&spec).is_ok());
    }

    #[test]
    fn validate_accepts_minimal_spec() {
        let spec = base_spec();
        let factory = ClickHouseSinkFactory;
        assert!(factory.validate_spec(&spec).is_ok());
    }

    #[test]
    fn validate_accepts_full_spec() {
        let mut spec = base_spec();
        spec.params
            .insert("port".into(), Value::Number(9440.into()));
        spec.params
            .insert("password".into(), Value::String("secret".into()));
        spec.params
            .insert("timeout_secs".into(), Value::Number(60.into()));
        spec.params
            .insert("max_retries".into(), Value::Number(5.into()));
        let factory = ClickHouseSinkFactory;
        assert!(factory.validate_spec(&spec).is_ok());
    }

    #[test]
    fn test_sink_def() {
        let factory = ClickHouseSinkFactory;
        let def = factory.sink_def();

        assert_eq!(def.id, "clickhouse_sink");
        assert_eq!(def.kind, "clickhouse");
        assert_eq!(def.scope, ConnectorScope::Sink);
        assert_eq!(
            def.origin,
            Some("wp-connectors:clickhouse_sink".to_string())
        );

        // 验证 allow_override 包含所有参数
        let expected_params = vec![
            "host",
            "port",
            "database",
            "table",
            "username",
            "password",
            "timeout_secs",
            "max_retries",
        ];
        for param in expected_params {
            assert!(
                def.allow_override.contains(&param.to_string()),
                "allow_override should contain {}",
                param
            );
        }

        // 验证 default_params 包含所有默认值
        assert!(def.default_params.contains_key("host"));
        assert!(def.default_params.contains_key("port"));
        assert!(def.default_params.contains_key("database"));
        assert!(def.default_params.contains_key("table"));
        assert!(def.default_params.contains_key("username"));
        assert!(def.default_params.contains_key("password"));
        assert!(def.default_params.contains_key("timeout_secs"));
        assert!(def.default_params.contains_key("max_retries"));
    }

    #[test]
    fn test_default_params_values() {
        let params = clickhouse_defaults();

        assert_eq!(
            params.get("host").and_then(Value::as_str),
            Some("localhost")
        );
        assert_eq!(params.get("port").and_then(Value::as_u64), Some(8123));
        assert_eq!(
            params.get("database").and_then(Value::as_str),
            Some("default")
        );
        assert_eq!(params.get("table").and_then(Value::as_str), Some("wp_logs"));
        assert_eq!(
            params.get("username").and_then(Value::as_str),
            Some("default")
        );
        assert_eq!(params.get("password").and_then(Value::as_str), Some(""));
        assert_eq!(params.get("timeout_secs").and_then(Value::as_u64), Some(30));
        assert_eq!(params.get("max_retries").and_then(Value::as_i64), Some(3));
    }
}
