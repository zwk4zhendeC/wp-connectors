use crate::http::{HTTPSink, HTTPSinkConfig};
use async_trait::async_trait;
use serde_json::{Value, json};
use std::collections::HashMap;
use wp_connector_api::{
    ConnectorDef, ConnectorScope, ParamMap, SinkBuildCtx, SinkDefProvider, SinkError, SinkFactory,
    SinkHandle, SinkReason, SinkResult, SinkSpec,
};
use wp_model_core::model::fmt_def::TextFmt;

pub struct HTTPSinkFactory;

#[async_trait]
impl SinkFactory for HTTPSinkFactory {
    fn kind(&self) -> &'static str {
        "http"
    }

    fn validate_spec(&self, spec: &SinkSpec) -> SinkResult<()> {
        ensure_not_empty(spec, "endpoint")?;

        let endpoint = required_param(spec, "endpoint")?;
        if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
            return Err(
                SinkReason::sink("http.endpoint must start with http:// or https://").into(),
            );
        }

        if let Some(method) = optional_string(spec, "method") {
            validate_method(&method)?;
        }

        if let Some(timeout) = get_u64(spec, "timeout_secs")
            && timeout == 0
        {
            return Err(SinkReason::sink("http.timeout_secs must be > 0").into());
        }

        parse_sink_fmt(spec.params.get("fmt"))?;
        parse_headers(spec)?;

        Ok(())
    }

    async fn build(&self, spec: &SinkSpec, _ctx: &SinkBuildCtx) -> SinkResult<SinkHandle> {
        let endpoint = required_param(spec, "endpoint")?;
        let method = optional_string(spec, "method");
        if let Some(method_ref) = method.as_ref() {
            validate_method(method_ref)?;
        }

        let fmt = parse_sink_fmt(spec.params.get("fmt"))?;
        let content_type = optional_string(spec, "content_type");
        let username = optional_string(spec, "username");
        let password = optional_string(spec, "password");
        let timeout_secs = parse_u64_param(spec, &["timeout_secs", "timeout"])?;
        let max_retries = parse_i32_param(spec, &["max_retries", "retries"])?;
        let headers = parse_headers(spec)?;

        let cfg = HTTPSinkConfig::new(
            endpoint,
            method,
            Some(fmt),
            content_type,
            username,
            password,
            timeout_secs,
            max_retries,
            Some(headers),
        );

        let sink = HTTPSink::new(cfg).await.map_err(|err| {
            SinkError::from(SinkReason::sink(format!("init http sink failed: {err}")))
        })?;

        Ok(SinkHandle::new(Box::new(sink)))
    }
}

impl SinkDefProvider for HTTPSinkFactory {
    fn sink_def(&self) -> ConnectorDef {
        ConnectorDef {
            id: "http_sink".into(),
            kind: self.kind().into(),
            scope: ConnectorScope::Sink,
            allow_override: vec![
                "endpoint",
                "method",
                "fmt",
                "content_type",
                "username",
                "password",
                "timeout_secs",
                "timeout",
                "max_retries",
                "retries",
                "headers",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
            default_params: http_defaults(),
            origin: Some("wp-connectors:http_sink".into()),
        }
    }
}

fn ensure_not_empty(spec: &SinkSpec, key: &str) -> SinkResult<()> {
    let value = spec
        .params
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_string();
    if value.is_empty() {
        return Err(SinkReason::sink(format!("http.{key} must not be empty")).into());
    }
    Ok(())
}

fn required_param(spec: &SinkSpec, key: &str) -> SinkResult<String> {
    spec.params
        .get(key)
        .and_then(Value::as_str)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| SinkReason::sink(format!("http.{key} must not be empty")).into())
}

fn optional_string(spec: &SinkSpec, key: &str) -> Option<String> {
    spec.params
        .get(key)
        .and_then(Value::as_str)
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn get_u64(spec: &SinkSpec, key: &str) -> Option<u64> {
    if let Some(value) = spec.params.get(key) {
        if let Some(n) = value.as_u64() {
            return Some(n);
        }
        if let Some(s) = value.as_str()
            && let Ok(parsed) = s.trim().parse::<u64>()
        {
            return Some(parsed);
        }
    }
    None
}

fn parse_u64_param(spec: &SinkSpec, keys: &[&str]) -> SinkResult<Option<u64>> {
    for key in keys {
        if let Some(value) = get_u64(spec, key) {
            if value == 0 {
                return Err(SinkReason::sink(format!("http.{key} must be > 0")).into());
            }
            return Ok(Some(value));
        }
    }
    Ok(None)
}

fn parse_i32_param(spec: &SinkSpec, keys: &[&str]) -> SinkResult<Option<i32>> {
    for key in keys {
        if let Some(value) = spec.params.get(*key) {
            if let Some(n) = value.as_i64() {
                let converted = i32::try_from(n)
                    .map_err(|_| SinkReason::sink(format!("http.{key} exceeds i32 range")))?;
                return Ok(Some(converted));
            }
            if let Some(s) = value.as_str() {
                let parsed = s.trim().parse::<i32>().map_err(|_| {
                    SinkReason::sink(format!("http.{key} must be a valid i32 value"))
                })?;
                return Ok(Some(parsed));
            }
        }
    }
    Ok(None)
}

fn validate_method(method: &str) -> SinkResult<()> {
    let normalized = method.trim().to_ascii_uppercase();
    let ok = matches!(normalized.as_str(), "POST" | "PUT" | "PATCH");
    if !ok {
        return Err(SinkReason::sink("http.method must be POST, PUT or PATCH").into());
    }
    Ok(())
}

fn parse_sink_fmt(value: Option<&Value>) -> SinkResult<TextFmt> {
    match value {
        None => Ok(HTTPSinkConfig::default_fmt()),
        Some(Value::String(raw)) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return Err(SinkReason::sink("http.fmt must not be empty").into());
            }
            let ok = matches!(
                trimmed,
                "json" | "csv" | "show" | "kv" | "raw" | "proto" | "proto-text"
            );
            if !ok {
                return Err(SinkReason::sink(format!(
                    "invalid fmt: '{}'; allowed: json,csv,show,kv,raw,proto,proto-text",
                    trimmed
                ))
                .into());
            }
            Ok(TextFmt::from(trimmed))
        }
        Some(_) => Err(SinkReason::sink("http.fmt must be a string").into()),
    }
}

fn parse_headers(spec: &SinkSpec) -> SinkResult<HashMap<String, String>> {
    if let Some(headers_value) = spec.params.get("headers") {
        let obj = headers_value
            .as_object()
            .ok_or_else(|| SinkReason::sink("http.headers must be an object"))?;

        let mut headers = HashMap::new();
        for (k, v) in obj {
            let value = v
                .as_str()
                .ok_or_else(|| SinkReason::sink(format!("http.headers.{k} must be a string")))?;
            headers.insert(k.clone(), value.to_string());
        }
        return Ok(headers);
    }

    Ok(HashMap::new())
}

fn http_defaults() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("endpoint".into(), json!("http://localhost:8080/ingest"));
    params.insert("method".into(), json!(HTTPSinkConfig::default_method()));
    params.insert(
        "fmt".into(),
        json!(HTTPSinkConfig::default_fmt().to_string()),
    );
    params.insert(
        "content_type".into(),
        json!(HTTPSinkConfig::default_content_type()),
    );
    params.insert("username".into(), json!(""));
    params.insert("password".into(), json!(""));
    params.insert(
        "timeout_secs".into(),
        json!(HTTPSinkConfig::default_timeout_secs()),
    );
    params.insert(
        "max_retries".into(),
        json!(HTTPSinkConfig::default_max_retries()),
    );
    params
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::collections::BTreeMap;

    fn base_spec() -> SinkSpec {
        let mut params = BTreeMap::new();
        params.insert(
            "endpoint".into(),
            Value::String("http://localhost:8080/ingest".into()),
        );
        SinkSpec {
            name: "http_sink".into(),
            kind: "http".into(),
            connector_id: String::new(),
            group: "test".into(),
            params,
            filter: None,
        }
    }

    #[test]
    fn validate_rejects_empty_endpoint() {
        let mut spec = base_spec();
        spec.params
            .insert("endpoint".into(), Value::String("".into()));

        let factory = HTTPSinkFactory;
        assert!(factory.validate_spec(&spec).is_err());
    }

    #[test]
    fn validate_rejects_invalid_endpoint() {
        let mut spec = base_spec();
        spec.params.insert(
            "endpoint".into(),
            Value::String("tcp://localhost:8080/ingest".into()),
        );

        let factory = HTTPSinkFactory;
        assert!(factory.validate_spec(&spec).is_err());
    }

    #[test]
    fn validate_rejects_invalid_method() {
        let mut spec = base_spec();
        spec.params
            .insert("method".into(), Value::String("GET".into()));

        let factory = HTTPSinkFactory;
        assert!(factory.validate_spec(&spec).is_err());
    }

    #[test]
    fn validate_rejects_invalid_fmt() {
        let mut spec = base_spec();
        spec.params
            .insert("fmt".into(), Value::String("xml".into()));

        let factory = HTTPSinkFactory;
        assert!(factory.validate_spec(&spec).is_err());
    }

    #[test]
    fn validate_rejects_zero_timeout() {
        let mut spec = base_spec();
        spec.params
            .insert("timeout_secs".into(), Value::Number(0.into()));

        let factory = HTTPSinkFactory;
        assert!(factory.validate_spec(&spec).is_err());
    }

    #[test]
    fn validate_rejects_non_string_header_value() {
        let mut spec = base_spec();
        spec.params.insert(
            "headers".into(),
            serde_json::json!({
                "X-API-Key": 123
            }),
        );

        let factory = HTTPSinkFactory;
        assert!(factory.validate_spec(&spec).is_err());
    }

    #[test]
    fn validate_accepts_minimal_spec() {
        let spec = base_spec();
        let factory = HTTPSinkFactory;
        assert!(factory.validate_spec(&spec).is_ok());
    }

    #[test]
    fn kind_is_http() {
        let factory = HTTPSinkFactory;
        assert_eq!(factory.kind(), "http");
    }
}
