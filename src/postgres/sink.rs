use async_trait::async_trait;
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Builder;
use wp_connector_api::{
    AsyncCtrl, AsyncRawDataSink, AsyncRecordSink, SinkError, SinkReason, SinkResult,
};
use wp_log::error_data;
use wp_model_core::model::{DataRecord, DataType};

pub struct PostgresSink {
    pub db: DatabaseConnection,
    pub table: String,
    pub cloumn_name: Vec<String>,
    pub values: Vec<String>,
}

impl PostgresSink {
    pub fn new(
        db: DatabaseConnection,
        table: String,
        cloumn_name: Vec<String>,
        batch: Option<usize>,
    ) -> Self {
        Self {
            db,
            table,
            cloumn_name,
            values: Vec::with_capacity(batch.unwrap_or(1024)),
        }
    }

    fn base_insert_prefix(&self) -> String {
        // 使用 INSERT IGNORE：若数据库已写入但客户端因断连未收到响应，重试时避免主键/唯一键冲突
        format!(
            "INSERT INTO {} ({}) VALUES ",
            self.table,
            self.cloumn_name
                .iter()
                .map(|s| format!("\"{}\"", s))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    fn format_values_tuple(&self, record: &DataRecord) -> String {
        let field_map: HashMap<&str, String> = record
            .items
            .iter()
            .filter(|f| *f.get_meta() != DataType::Ignore)
            .map(|f| (f.get_name(), f.get_value().to_string()))
            .collect();
        let values: Vec<String> = self
            .cloumn_name
            .iter()
            .map(|col_name| match field_map.get(col_name.as_str()) {
                Some(field) => format!("'{}'", field.replace("'", "''")),
                None => {
                    error_data!("Warning: Missing field for column '{}'", col_name);
                    "NULL".to_string()
                }
            })
            .collect();
        format!("({})", values.join(", "))
    }

    /// 推送缓存区的数据
    async fn flush_pending_sqls(
        &self,
        pending_sqls: Vec<String>,
        backend: DatabaseBackend,
    ) -> SinkResult<()> {
        if pending_sqls.is_empty() {
            return Ok(());
        }
        let existing = self.db.clone();
        tokio::task::spawn_blocking(move || -> SinkResult<()> {
            let runtime = Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| {
                    SinkError::from(SinkReason::Sink(format!(
                        "build runtime for postgres flush fail: {}",
                        e
                    )))
                })?;

            runtime.block_on(async move {
                let conn = existing.clone();

                for sql in pending_sqls {
                    let state = Statement::from_string(backend, sql.clone());
                    match conn.execute(state).await {
                        Ok(_) => {}
                        Err(e) => {
                            return Err(SinkError::from(SinkReason::Sink(format!(
                                "postgres execute fail: {}, excute sql: {}",
                                e, sql
                            ))));
                        }
                    }
                }
                Ok(())
            })
        })
        .await
        .map_err(|e| {
            SinkError::from(SinkReason::Sink(format!(
                "postgres flush join error: {}",
                e
            )))
        })?
    }
}

#[async_trait]
impl AsyncCtrl for PostgresSink {
    async fn stop(&mut self) -> SinkResult<()> {
        // 将待写入的数据提前组装为 SQL 字符串，避免对 self.values 的借用贯穿异步等待
        let backend = self.db.get_database_backend();
        let mut pending_sqls: Vec<String> = Vec::new();
        if !self.values.is_empty() {
            // 单条 INSERT + 多个 VALUES（不加分号，兼容性更好）
            let mut sql = self.base_insert_prefix();
            sql.push_str(&self.values.join(","));
            pending_sqls.push(sql);
        }
        // 清空缓存，避免重复写
        if pending_sqls.is_empty() {
            return Ok(());
        }
        self.flush_pending_sqls(pending_sqls, backend).await
    }
    async fn reconnect(&mut self) -> SinkResult<()> {
        self.db.ping().await.map_err(|e| {
            SinkError::from(SinkReason::Sink(format!("reconnect postgres fail: {}", e)))
        })?;
        Ok(())
    }
}

#[async_trait]
impl AsyncRecordSink for PostgresSink {
    async fn sink_record(&mut self, data: &DataRecord) -> SinkResult<()> {
        let raw = self.format_values_tuple(data);
        self.values.push(raw);

        if !self.values.is_empty() {
            // 单条 INSERT + 多个 VALUES（不加分号，兼容性更好）
            let mut sql = self.base_insert_prefix();
            sql.push_str(&self.values.join(","));
            if let Err(e) = self.db.execute_unprepared(sql.as_str()).await {
                return Err(SinkError::from(SinkReason::Sink(format!(
                    "postgres exec cloumns:{:?}, fail: {}, sql: {}",
                    self.cloumn_name, e, sql
                ))));
            }
        }
        self.values.clear();
        Ok(())
    }

    async fn sink_records(&mut self, data: Vec<Arc<DataRecord>>) -> SinkResult<()> {
        let mut raws = Vec::with_capacity(data.len());
        for record in data {
            raws.push(self.format_values_tuple(record.as_ref()));
        }
        self.values.extend(raws);
        if !self.values.is_empty() {
            // 单条 INSERT + 多个 VALUES
            let mut sql = self.base_insert_prefix();
            sql.push_str(&self.values.join(","));
            if let Err(e) = self.db.execute_unprepared(sql.as_str()).await {
                return Err(SinkError::from(SinkReason::Sink(format!(
                    "postgres exec cloumns:{:?}, fail: {}, sql: {}",
                    self.cloumn_name, e, sql
                ))));
            }
        }
        self.values.clear();
        Ok(())
    }
}

#[async_trait]
impl AsyncRawDataSink for PostgresSink {
    async fn sink_str(&mut self, _data: &str) -> SinkResult<()> {
        Err(SinkError::from(SinkReason::Sink(
            "postgres sink does not accept raw input".into(),
        )))
    }
    async fn sink_bytes(&mut self, _data: &[u8]) -> SinkResult<()> {
        Err(SinkError::from(SinkReason::Sink(
            "postgres sink does not accept raw bytes".into(),
        )))
    }

    async fn sink_str_batch(&mut self, _data: Vec<&str>) -> SinkResult<()> {
        Err(SinkError::from(SinkReason::Sink(
            "postgres sink does not accept raw input".into(),
        )))
    }
    async fn sink_bytes_batch(&mut self, _data: Vec<&[u8]>) -> SinkResult<()> {
        Err(SinkError::from(SinkReason::Sink(
            "postgres sink does not accept raw bytes".into(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::PostgresSink;
    use sea_orm::DatabaseConnection;
    use wp_model_core::model::{DataField, DataRecord};

    fn make_sink(table: &str, columns: Vec<&str>) -> PostgresSink {
        PostgresSink::new(
            DatabaseConnection::default(),
            table.to_string(),
            columns.into_iter().map(|s| s.to_string()).collect(),
            Some(8),
        )
    }

    #[test]
    fn postgres_sink_base_insert_prefix() {
        let sink = make_sink("users", vec!["name", "age"]);
        let sql = sink.base_insert_prefix();
        assert_eq!(sql, "INSERT INTO users (\"name\", \"age\") VALUES ");
    }

    #[test]
    fn postgres_sink_format_values_tuple() {
        let sink = make_sink("users", vec!["name", "age", "note"]);
        let mut record = DataRecord::default();
        record.append(DataField::from_chars("name", "O'Reilly"));
        record.append(DataField::from_digit("age", 42));
        record.append(DataField::from_ignore("unused"));

        let values = sink.format_values_tuple(&record);
        assert_eq!(values, "('O''Reilly', '42', NULL)");
    }
}
