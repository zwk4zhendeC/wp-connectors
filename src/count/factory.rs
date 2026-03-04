use async_trait::async_trait;
use wp_connector_api::{
    ConnectorDef, ConnectorScope, ParamMap, SinkBuildCtx, SinkDefProvider, SinkError, SinkFactory,
    SinkHandle, SinkReason, SinkResult, SinkSpec,
};

use crate::count::CountSink;

pub struct CountSinkFactory;

#[async_trait]
impl SinkFactory for CountSinkFactory {
    fn kind(&self) -> &'static str {
        "count"
    }

    fn validate_spec(&self, _spec: &SinkSpec) -> SinkResult<()> {
        Ok(())
    }

    async fn build(&self, _spec: &SinkSpec, _ctx: &SinkBuildCtx) -> SinkResult<SinkHandle> {
        let sink = CountSink::new().await.map_err(|err| {
            SinkError::from(SinkReason::sink(format!("init count sink failed: {err}")))
        })?;

        Ok(SinkHandle::new(Box::new(sink)))
    }
}

impl SinkDefProvider for CountSinkFactory {
    fn sink_def(&self) -> ConnectorDef {
        ConnectorDef {
            id: "count_sink".into(),
            kind: self.kind().into(),
            scope: ConnectorScope::Sink,
            allow_override: vec![].into_iter().map(str::to_string).collect(),
            default_params: count_defaults(),
            origin: Some("wp-connectors:count_sink".into()),
        }
    }
}

fn count_defaults() -> ParamMap {
    ParamMap::new()
}
