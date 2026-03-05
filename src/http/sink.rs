use crate::http::config::HTTPSinkConfig;
use crate::utils::time_stat_utils::TimeStatUtils;
use async_trait::async_trait;
use reqwest::{Client, Method};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::time::sleep;
use wp_connector_api::{
    AsyncCtrl, AsyncRawDataSink, AsyncRecordSink, SinkError, SinkReason, SinkResult,
};
use wp_data_fmt::{FormatType, RecordFormatter};
use wp_model_core::model::{DataRecord, fmt_def::TextFmt};

static INSTANCE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
const BASE_BACKOFF_MS: u64 = 10;
#[cfg(not(test))]
const BASE_BACKOFF_MS: u64 = 1000;

pub struct HTTPSink {
    client: Client,
    endpoint: String,
    method: Method,
    fmt: TextFmt,
    content_type: String,
    username: String,
    password: String,
    headers: HashMap<String, String>,
    max_retries: i32,
    instance_id: u64,
    time_stats: TimeStatUtils,
}

impl HTTPSink {
    pub async fn new(config: HTTPSinkConfig) -> anyhow::Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .no_proxy()
            .build()?;

        let method = Method::from_bytes(config.method.as_bytes())?;

        let instance_id = INSTANCE_COUNTER.fetch_add(1, Ordering::SeqCst);

        Ok(Self {
            client,
            endpoint: config.endpoint,
            method,
            fmt: config.fmt,
            content_type: config.content_type,
            username: config.username,
            password: config.password,
            headers: config.headers,
            max_retries: config.max_retries,
            instance_id,
            time_stats: TimeStatUtils::new(),
        })
    }

    fn records_to_payload(&self, records: &[Arc<DataRecord>]) -> String {
        let fmt = FormatType::from(&self.fmt);
        records
            .iter()
            .map(|record| fmt.fmt_record(record.as_ref()))
            .collect::<Vec<_>>()
            .join("\n")
    }

    async fn send_with_retry(&self, payload: String) -> SinkResult<()> {
        let mut retries = 0;
        let max_retries = if self.max_retries < 0 {
            i32::MAX
        } else {
            self.max_retries
        };

        loop {
            let mut request = self
                .client
                .request(self.method.clone(), &self.endpoint)
                .header("Content-Type", &self.content_type)
                .body(payload.clone());

            if !self.username.is_empty() {
                request = request.basic_auth(&self.username, Some(&self.password));
            }

            for (key, value) in &self.headers {
                request = request.header(key, value);
            }

            match request.send().await {
                Ok(response) => {
                    let status = response.status();
                    let body = response
                        .text()
                        .await
                        .unwrap_or_else(|_| "failed to read response body".to_string());

                    if status.is_success() {
                        return Ok(());
                    }

                    if status.is_client_error() {
                        return Err(sink_error(format!(
                            "client error: status={}, body={}",
                            status, body
                        )));
                    }

                    log::warn!(
                        "http sink server error: status={}, body={}, retry={}/{}",
                        status,
                        body,
                        retries + 1,
                        max_retries
                    );
                }
                Err(err) => {
                    log::warn!(
                        "http sink request failed: {}, retry={}/{}",
                        err,
                        retries + 1,
                        max_retries
                    );
                }
            }

            retries += 1;
            if retries >= max_retries {
                return Err(sink_error(format!(
                    "max retries ({}) exceeded",
                    max_retries
                )));
            }

            let backoff_ms = BASE_BACKOFF_MS
                .saturating_mul(2_u64.pow(retries.min(10) as u32))
                .max(BASE_BACKOFF_MS);
            sleep(Duration::from_millis(backoff_ms)).await;
        }
    }
}

#[async_trait]
impl AsyncCtrl for HTTPSink {
    async fn stop(&mut self) -> SinkResult<()> {
        Ok(())
    }

    async fn reconnect(&mut self) -> SinkResult<()> {
        Ok(())
    }
}

#[async_trait]
impl AsyncRecordSink for HTTPSink {
    async fn sink_record(&mut self, data: &DataRecord) -> SinkResult<()> {
        self.sink_records(vec![Arc::new(data.clone())]).await
    }

    async fn sink_records(&mut self, data: Vec<Arc<DataRecord>>) -> SinkResult<()> {
        if data.is_empty() {
            return Ok(());
        }

        self.time_stats.start_stat(data.len() as u64);

        let payload = self.records_to_payload(&data);
        let result = self.send_with_retry(payload).await;

        self.time_stats.end_stat();
        self.time_stats
            .println(&format!("HTTPSink-{}", self.instance_id));

        result
    }
}

#[async_trait]
impl AsyncRawDataSink for HTTPSink {
    async fn sink_str(&mut self, _data: &str) -> SinkResult<()> {
        Err(sink_error("http sink does not accept raw text input"))
    }

    async fn sink_bytes(&mut self, _data: &[u8]) -> SinkResult<()> {
        Err(sink_error("http sink does not accept raw byte input"))
    }

    async fn sink_str_batch(&mut self, _data: Vec<&str>) -> SinkResult<()> {
        Err(sink_error("http sink does not accept raw batch input"))
    }

    async fn sink_bytes_batch(&mut self, _data: Vec<&[u8]>) -> SinkResult<()> {
        Err(sink_error("http sink does not accept raw batch byte input"))
    }
}

fn sink_error(msg: impl Into<String>) -> SinkError {
    SinkError::from(SinkReason::Sink(msg.into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::prelude::*;
    use std::net::TcpListener;
    use wp_connector_api::{AsyncRawDataSink, AsyncRecordSink};
    use wp_model_core::model::{DataField, DataRecord};

    async fn create_test_sink(endpoint: String, max_retries: i32) -> HTTPSink {
        let cfg = HTTPSinkConfig::new(
            endpoint,
            Some("POST".into()),
            Some(TextFmt::Json),
            Some("application/x-ndjson".into()),
            None,
            None,
            Some(2),
            Some(max_retries),
            None,
        );

        HTTPSink::new(cfg)
            .await
            .expect("failed to create http sink")
    }

    fn create_record() -> DataRecord {
        let mut record = DataRecord::default();
        record.append(DataField::from_chars("message", "hello-http"));
        record
    }

    fn can_bind_localhost() -> bool {
        TcpListener::bind("127.0.0.1:0").is_ok()
    }

    #[tokio::test]
    async fn sink_record_success() {
        if !can_bind_localhost() {
            return;
        }

        let server = MockServer::start_async().await;
        let mock = server.mock(|when, then| {
            when.method(POST).path("/ingest");
            then.status(200);
        });

        let mut sink = create_test_sink(format!("{}/ingest", server.base_url()), 3).await;
        let record = create_record();

        let result = sink.sink_record(&record).await;
        assert!(result.is_ok());
        assert_eq!(mock.calls(), 1);
    }

    #[tokio::test]
    async fn sink_record_does_not_retry_on_4xx() {
        if !can_bind_localhost() {
            return;
        }

        let server = MockServer::start_async().await;
        let mock = server.mock(|when, then| {
            when.method(POST).path("/ingest");
            then.status(400).body("bad request");
        });

        let mut sink = create_test_sink(format!("{}/ingest", server.base_url()), 3).await;
        let record = create_record();

        let result = sink.sink_record(&record).await;
        assert!(result.is_err());
        assert_eq!(mock.calls(), 1);
    }

    #[tokio::test]
    async fn sink_record_retries_on_5xx() {
        if !can_bind_localhost() {
            return;
        }

        let server = MockServer::start_async().await;
        let mock = server.mock(|when, then| {
            when.method(POST).path("/ingest");
            then.status(500).body("internal error");
        });

        let mut sink = create_test_sink(format!("{}/ingest", server.base_url()), 3).await;
        let record = create_record();

        let result = sink.sink_record(&record).await;
        assert!(result.is_err());
        assert_eq!(mock.calls(), 3);
    }

    #[tokio::test]
    async fn raw_input_is_rejected() {
        let mut sink = create_test_sink("http://localhost:8080/ingest".to_string(), 1).await;

        assert!(sink.sink_str("abc").await.is_err());
        assert!(sink.sink_bytes(b"abc").await.is_err());
        assert!(sink.sink_str_batch(vec!["a", "b"]).await.is_err());
        assert!(sink.sink_bytes_batch(vec![b"a", b"b"]).await.is_err());
    }
}
