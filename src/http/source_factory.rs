use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use wp_connector_api::{
    ConnectorDef, ConnectorScope, ParamMap, SourceBuildCtx, SourceDefProvider, SourceFactory,
    SourceHandle, SourceMeta, SourceReason, SourceResult, SourceSpec, SourceSvcIns,
};

use crate::http::source::{
    HttpSource, HttpSourceConfig, build_source_tags, http_source_queue_capacity,
};

pub struct HttpSourceFactory;

#[async_trait]
impl SourceFactory for HttpSourceFactory {
    fn kind(&self) -> &'static str {
        "http"
    }

    fn validate_spec(&self, spec: &SourceSpec) -> SourceResult<()> {
        build_http_source_config(spec)?;
        Ok(())
    }

    async fn build(&self, spec: &SourceSpec, _ctx: &SourceBuildCtx) -> SourceResult<SourceSvcIns> {
        let config = build_http_source_config(spec)?;
        let (sender, receiver) = mpsc::channel(http_source_queue_capacity());
        HttpSource::register(&config, sender)
            .await
            .map_err(|err| SourceReason::Other(err.to_string()))?;

        let meta_tags = build_source_tags(&spec.tags, &config);
        let source = HttpSource::new(spec.name.clone(), meta_tags.clone(), config, receiver);

        let mut meta = SourceMeta::new(spec.name.clone(), spec.kind.clone());
        meta.tags = meta_tags;

        Ok(SourceSvcIns::new().with_sources(vec![SourceHandle::new(Box::new(source), meta)]))
    }
}

impl SourceDefProvider for HttpSourceFactory {
    fn source_def(&self) -> ConnectorDef {
        ConnectorDef {
            id: "http_src".into(),
            kind: self.kind().into(),
            scope: ConnectorScope::Source,
            allow_override: vec!["port", "path"]
                .into_iter()
                .map(str::to_string)
                .collect(),
            default_params: http_source_defaults(),
            origin: Some("wp-connectors:http_source".into()),
        }
    }
}

fn build_http_source_config(spec: &SourceSpec) -> SourceResult<HttpSourceConfig> {
    let port = required_port(spec, "port")?;
    let path = required_path(spec, "path")?;
    Ok(HttpSourceConfig { port, path })
}

fn required_port(spec: &SourceSpec, key: &str) -> SourceResult<u16> {
    let port = spec
        .params
        .get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| SourceReason::Other(format!("http.{key} must be an integer")))?;

    if port == 0 || port > u16::MAX as u64 {
        return Err(SourceReason::Other(format!("http.{key} must be in 1..=65535")).into());
    }

    Ok(port as u16)
}

fn required_path(spec: &SourceSpec, key: &str) -> SourceResult<String> {
    let path = spec
        .params
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|path| !path.is_empty())
        .ok_or_else(|| SourceReason::Other(format!("http.{key} must not be empty")))?;

    if !path.starts_with('/') {
        return Err(SourceReason::Other(format!("http.{key} must start with '/'")).into());
    }

    Ok(path.to_string())
}

fn http_source_defaults() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("port".into(), json!(18080));
    params.insert("path".into(), json!("/ingest"));
    params
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn build_spec(params: BTreeMap<String, Value>) -> SourceSpec {
        SourceSpec {
            name: "http_source".into(),
            kind: "http".into(),
            connector_id: "connector".into(),
            params,
            tags: vec![],
        }
    }

    #[test]
    fn validate_requires_port_and_path() {
        let factory = HttpSourceFactory;
        let err = factory
            .validate_spec(&build_spec(BTreeMap::new()))
            .expect_err("missing params should fail");
        assert!(err.to_string().contains("http.port must be an integer"));
    }

    #[test]
    fn validate_rejects_path_without_leading_slash() {
        let factory = HttpSourceFactory;
        let spec = build_spec(BTreeMap::from([
            ("port".into(), json!(18080)),
            ("path".into(), json!("ingest")),
        ]));
        let err = factory
            .validate_spec(&spec)
            .expect_err("path without slash should fail");
        assert!(err.to_string().contains("http.path must start with '/'"));
    }

    #[test]
    fn validate_accepts_valid_config() {
        let factory = HttpSourceFactory;
        let spec = build_spec(BTreeMap::from([
            ("port".into(), json!(18080)),
            ("path".into(), json!("/ingest")),
        ]));
        factory.validate_spec(&spec).expect("valid spec");
    }
}
