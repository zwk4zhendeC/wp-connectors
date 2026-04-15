use std::time::Duration;

use async_trait::async_trait;
use serde_json::json;
use wp_connector_api::{
    ConnectorDef, ConnectorScope, ParamMap, SinkBuildCtx, SinkDefProvider, SinkError, SinkFactory,
    SinkHandle, SinkReason, SinkResult, SinkSpec,
};

use super::config::VictoriaMetric;
use super::exporter::VictoriaMetricExporter;

pub struct VictoriaMetricFactory;

#[async_trait]
impl SinkFactory for VictoriaMetricFactory {
    fn kind(&self) -> &'static str {
        "victoriametrics"
    }
    fn validate_spec(&self, spec: &SinkSpec) -> SinkResult<()> {
        let insert_url = spec
            .params
            .get("insert_url")
            .and_then(|v| v.as_str())
            .or_else(|| spec.params.get("endpoint").and_then(|v| v.as_str()))
            .unwrap_or("");
        if insert_url.trim().is_empty() {
            return Err(SinkReason::sink("victoriametrics.insert_url must not be empty").into());
        }
        Ok(())
    }
    async fn build(&self, spec: &SinkSpec, _ctx: &SinkBuildCtx) -> SinkResult<SinkHandle> {
        let mut conf = VictoriaMetric::default();
        if let Some(v) = spec.params.get("flush_interval_secs") {
            if let Some(n) = v.as_f64() {
                if n > 0.0 {
                    conf.flush_interval_secs = n;
                }
            } else if let Some(s) = v.as_str()
                && let Ok(n) = s.parse::<f64>()
            {
                conf.flush_interval_secs = n;
            }
        }
        if let Some(s) = spec
            .params
            .get("insert_url")
            .and_then(|v| v.as_str())
            .or_else(|| spec.params.get("endpoint").and_then(|v| v.as_str()))
        {
            conf.insert_url = s.to_string();
        }

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .map_err(|err| {
                SinkError::from(SinkReason::sink(format!(
                    "build victoriametric client failed: {err}"
                )))
            })?;
        let mut sink = VictoriaMetricExporter::new(
            conf.insert_url.clone(),
            client,
            Duration::from_secs_f64(conf.flush_interval_secs),
        );
        // 启动定时 flush 任务：计数器收集与推送解耦，
        sink.start_flush_task();
        Ok(SinkHandle::new(Box::new(sink)))
    }
}

impl SinkDefProvider for VictoriaMetricFactory {
    fn sink_def(&self) -> ConnectorDef {
        ConnectorDef {
            id: "victoriametrics_sink".into(),
            kind: self.kind().into(),
            scope: ConnectorScope::Sink,
            allow_override: vec!["insert_url", "flush_interval_secs"]
                .into_iter()
                .map(str::to_string)
                .collect(),
            default_params: victoriametric_defaults(),
            origin: Some("wp-connectors:victoriametrics_sink".into()),
        }
    }
}

fn victoriametric_defaults() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert(
        "insert_url".into(),
        json!("http://127.0.0.1:8428/api/v1/import/prometheus"),
    );
    // flush_interval_secs 决定推送到 VictoriaMetrics 的时间分辨率，
    // 1s 可获得秒级数据点，适合 rate([20s+]) 的稳定计算。
    params.insert("flush_interval_secs".into(), json!(1));
    params
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sink_spec(params: &[(&str, serde_json::Value)]) -> SinkSpec {
        let mut map = ParamMap::new();
        for (key, value) in params {
            map.insert((*key).to_string(), value.clone());
        }
        SinkSpec {
            group: "g".into(),
            name: "vm".into(),
            kind: "victoriametrics".into(),
            connector_id: "victoriametrics_sink".into(),
            params: map,
            filter: None,
        }
    }

    #[test]
    fn sink_def_matches_official_template() {
        let def = VictoriaMetricFactory.sink_def();
        assert_eq!(def.id, "victoriametrics_sink");
        assert_eq!(
            def.allow_override,
            vec!["insert_url".to_string(), "flush_interval_secs".to_string()]
        );
        assert_eq!(
            def.default_params
                .get("insert_url")
                .and_then(|v| v.as_str()),
            Some("http://127.0.0.1:8428/api/v1/import/prometheus")
        );
        assert_eq!(
            def.default_params
                .get("flush_interval_secs")
                .and_then(|v| v.as_i64()),
            Some(1)
        );
    }

    #[test]
    fn validate_accepts_insert_url() {
        let spec = sink_spec(&[(
            "insert_url",
            json!("http://127.0.0.1:8428/api/v1/import/prometheus"),
        )]);
        assert!(VictoriaMetricFactory.validate_spec(&spec).is_ok());
    }

    #[test]
    fn validate_keeps_legacy_endpoint_compatible() {
        let spec = sink_spec(&[("endpoint", json!("http://localhost:8480"))]);
        assert!(VictoriaMetricFactory.validate_spec(&spec).is_ok());
    }
}
