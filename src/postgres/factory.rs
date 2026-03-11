use async_trait::async_trait;
use sea_orm::{ConnectOptions, Database};
use serde_json::json;
use std::time::Duration;
use wp_connector_api::{
    ConnectorDef, ConnectorScope, ParamMap, SinkBuildCtx, SinkDefProvider, SinkError, SinkFactory,
    SinkHandle, SinkReason, SinkResult, SinkSpec,
};

use crate::postgres::{PostgresSink, config::PostgresConf};

pub struct PostgresSinkFactory;

#[async_trait]
impl SinkFactory for PostgresSinkFactory {
    fn kind(&self) -> &'static str {
        "postgres"
    }
    fn validate_spec(&self, spec: &SinkSpec) -> SinkResult<()> {
        let endpoint = spec
            .params
            .get("endpoint")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if endpoint.trim().is_empty() {
            return Err(SinkReason::sink("postgres.endpoint must not be empty").into());
        }
        let database = spec
            .params
            .get("database")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if database.trim().is_empty() {
            return Err(SinkReason::sink("postgres.database must not be empty").into());
        }
        if let Some(i) = spec.params.get("batch").and_then(|v| v.as_i64())
            && i <= 0
        {
            return Err(SinkReason::sink("postgres.batch must be > 0").into());
        }
        Ok(())
    }
    async fn build(&self, spec: &SinkSpec, _ctx: &SinkBuildCtx) -> SinkResult<SinkHandle> {
        // Build Postgres conf from flat params
        let mut conf = PostgresConf::default();
        if let Some(s) = spec.params.get("endpoint").and_then(|v| v.as_str()) {
            conf.endpoint = s.to_string();
        }
        if let Some(s) = spec.params.get("username").and_then(|v| v.as_str()) {
            conf.username = s.to_string();
        }
        if let Some(s) = spec.params.get("password").and_then(|v| v.as_str()) {
            conf.password = s.to_string();
        }
        if let Some(s) = spec.params.get("database").and_then(|v| v.as_str()) {
            conf.database = s.to_string();
        }
        if let Some(s) = spec.params.get("table").and_then(|v| v.as_str()) {
            conf.table = Some(s.to_string());
        }
        // Use unsigned extraction to match usize semantics
        if let Some(i) = spec.params.get("batch_size").and_then(|v| v.as_u64()) {
            conf.batch = Some(i as usize);
        }
        // columns 列表在新版配置中不在 conf 中，作为外部参数传入 sink
        let columns: Vec<String> = if let Some(arr) =
            spec.params.get("columns").and_then(|v| v.as_array())
        {
            let mut out = Vec::with_capacity(arr.len());
            for item in arr {
                if let Some(s) = item.as_str() {
                    out.push(s.to_string());
                } else {
                    return Err(SinkReason::sink("postgres.columns entries must be string").into());
                }
            }
            out
        } else {
            Vec::new()
        };
        let url = conf.get_database_url();
        let mut opt = ConnectOptions::new(url.clone());

        opt.max_connections(50)
            .min_connections(1)
            .connect_timeout(Duration::from_secs(8))
            .acquire_timeout(Duration::from_secs(8))
            .idle_timeout(Duration::from_secs(8))
            .max_lifetime(Duration::from_secs(8))
            .sqlx_logging(false)
            .map_sqlx_postgres_opts(|opt| opt.statement_cache_capacity(0))
            .sqlx_logging_level(log::LevelFilter::Info);
        let db = Database::connect(opt).await.map_err(|err| {
            SinkError::from(SinkReason::sink(format!("connect postgres fail: {err}")))
        })?;
        let table = conf.table.clone().unwrap_or_else(|| spec.name.clone());
        let sink = PostgresSink::new(db, table, columns);
        Ok(SinkHandle::new(Box::new(sink)))
    }
}

impl SinkDefProvider for PostgresSinkFactory {
    fn sink_def(&self) -> ConnectorDef {
        ConnectorDef {
            id: "postgres_sink".into(),
            kind: self.kind().into(),
            scope: ConnectorScope::Sink,
            allow_override: vec![
                "endpoint", "database", "table", "username", "batch", "columns",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
            default_params: postgres_sink_defaults(),
            origin: Some("wp-connectors:postgres_sink".into()),
        }
    }
}

fn postgres_sink_defaults() -> ParamMap {
    let mut params = ParamMap::new();
    params.insert("endpoint".into(), json!("postgres://localhost:5432"));
    params.insert("database".into(), json!("wp_data"));
    params.insert("table".into(), json!("wp_events"));
    params.insert("username".into(), json!("root"));
    params.insert("columns".into(), json!(["wp_event_id", "payload"]));
    params.insert("batch_size".into(), json!(1024));
    params
}
