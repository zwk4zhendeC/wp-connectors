//! Doris Sink 实现
//!
//! # 重要说明
//!
//! Doris 虽然兼容 MySQL 协议，但**不完全支持 MySQL 的预编译语句（Prepared Statements）**。
//! 因此本实现使用字符串拼接方式构建 SQL，但通过以下措施保证安全性：
//!
//! 1. **数据值转义** - 使用 `escape_sql_string()` 处理所有危险字符
//! 2. **标识符转义** - 使用 `quote_identifier()` 处理表名和列名
//! 3. **批量插入** - 保持高性能的多行 INSERT
//!
//! # SQL 注入防护
//!
//! - 所有用户数据通过 `escape_sql_string()` 转义（反斜杠、单引号、NULL、换行等）
//! - 所有标识符通过 `quote_identifier()` 包裹反引号
//! - 不直接拼接未转义的用户输入

use crate::doris::config::DorisSinkConfig;
use async_trait::async_trait;
use sqlx::{
    Row,
    mysql::{MySqlConnectOptions, MySqlPool, MySqlPoolOptions},
    raw_sql,
};
use std::sync::Arc;
use std::{
    collections::{HashMap, HashSet},
    // sync::atomic::{AtomicUsize, Ordering},
};
use wp_connector_api::{
    AsyncCtrl, AsyncRawDataSink, AsyncRecordSink, SinkError, SinkReason, SinkResult,
};
use wp_model_core::model::{DataRecord, DataType};

pub struct DorisSink {
    pub pool: MySqlPool,
    quoted_table: String,
    column_order: Vec<String>,
    quoted_columns: Vec<String>,
    column_set: HashSet<String>,
}

impl DorisSink {
    /// 构建 Doris Sink，负责拉起连接池、建库建表并缓存列信息。
    ///
    /// # args
    /// * `config` - Doris 连接与写入所需的完整配置。
    /// # return: `anyhow::Result<Self>` - 成功则返回初始化后的 sink。
    pub async fn new(config: DorisSinkConfig) -> anyhow::Result<Self> {
        create_database_if_missing(&config).await?;

        let db_opts = sanitize_options(
            config.database_dsn().parse::<MySqlConnectOptions>()?,
            &config.user,
            &config.password,
        );
        let pool = MySqlPoolOptions::new()
            .max_connections(config.pool_size.max(1))
            .connect_with(db_opts)
            .await?;

        ensure_table_exists(
            &pool,
            &config.database,
            &config.table,
            config.create_table.as_deref(),
        )
        .await?;

        let column_order = load_table_columns(&pool, &config.database, &config.table).await?;
        if column_order.is_empty() {
            anyhow::bail!("table `{}` has no columns", config.table);
        }
        let column_set = column_order.iter().cloned().collect::<HashSet<_>>();
        let quoted_columns = column_order
            .iter()
            .map(|name| quote_identifier(name))
            .collect::<Vec<_>>();
        let quoted_table = quote_identifier(&format!("{}.{}", config.database, config.table));

        Ok(Self {
            pool,
            quoted_table,
            column_order,
            quoted_columns,
            column_set,
        })
    }

    /// 生成固定的 INSERT 语句前缀（含表名和列名）。
    ///
    /// # return
    /// * `String` - 形如 `INSERT INTO db.table (col1,...) VALUES ` 的片段。
    fn base_insert_prefix(&self) -> String {
        format!(
            "INSERT INTO {} ({}) VALUES ",
            self.quoted_table,
            self.quoted_columns.join(", ")
        )
    }

    /// 将一条 [`DataRecord`] 转换成 `(v1, v2, ..)` 形式的 VALUES 片段。
    /// 使用安全的转义函数防止 SQL 注入。
    ///
    /// # args
    /// * `record` - 上层传入的数据记录。
    ///
    /// # return
    /// * `Option<String>` - 若存在可写字段则返回 VALUES 字符串，否则为 `None`。
    fn format_values_tuple(&self, record: &DataRecord) -> Option<String> {
        let mut field_map: HashMap<&str, String> = HashMap::new();
        for field in &record.items {
            if *field.get_meta() == DataType::Ignore {
                continue;
            }
            let name = field.get_name();
            if self.column_set.contains(name) {
                field_map.insert(name, field.get_value().to_string());
            }
        }
        if field_map.is_empty() {
            return None;
        }

        let values: Vec<String> = self
            .column_order
            .iter()
            .map(|column| match field_map.get(column.as_str()) {
                Some(value) => format!("'{}'", escape_sql_string(value)),
                None => "NULL".to_string(),
            })
            .collect();
        Some(format!("({})", values.join(", ")))
    }

    /// 将缓存的 VALUES 组成批量 INSERT 并写入 Doris。
    /// 注意：Doris 不完全支持 MySQL 预编译语句，因此使用字符串拼接 + 安全转义。
    ///
    /// # args
    /// * `sql_values` - VALUES 片段列表。
    ///
    /// # return
    /// * `SinkResult<()>` - 成功表示缓存已清空。
    async fn flush_pending(&mut self, sql_values: Vec<String>) -> SinkResult<()> {
        if sql_values.is_empty() {
            return Ok(());
        }

        let sql = format!("{}{}", self.base_insert_prefix(), sql_values.join(", "));
        raw_sql(&sql)
            .execute(&self.pool)
            .await
            .map_err(|e| sink_error(format!("doris insert fail: {}", e)))?;

        Ok(())
    }
}

#[async_trait]
impl AsyncCtrl for DorisSink {
    async fn stop(&mut self) -> SinkResult<()> {
        Ok(())
    }

    async fn reconnect(&mut self) -> SinkResult<()> {
        // Doris 不支持预编译，使用 raw_sql
        raw_sql("SELECT 1")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| sink_error(format!("doris reconnect fail: {}", e)))?;
        Ok(())
    }
}

// static REC_CNT: AtomicUsize = AtomicUsize::new(0);
// static SED_CNT: AtomicUsize = AtomicUsize::new(0);

#[async_trait]
impl AsyncRecordSink for DorisSink {
    async fn sink_record(&mut self, data: &DataRecord) -> SinkResult<()> {
        self.sink_records(vec![Arc::new(data.clone())]).await
    }

    async fn sink_records(&mut self, data: Vec<Arc<DataRecord>>) -> SinkResult<()> {
        if data.is_empty() {
            return Ok(());
        }
        // let cnt = REC_CNT.fetch_add(data.len(), Ordering::Relaxed) + data.len();
        // println!("doris已接收:{}", cnt);
        let sql_values = data
            .iter()
            .filter_map(|record| self.format_values_tuple(record.as_ref()))
            .collect();

        self.flush_pending(sql_values).await?;
        // let cnt = SED_CNT.fetch_add(data.len(), Ordering::Relaxed) + data.len();
        // println!("doris已发送:{}", cnt);
        return Ok(());
    }
}

#[async_trait]
impl AsyncRawDataSink for DorisSink {
    async fn sink_str(&mut self, _data: &str) -> SinkResult<()> {
        Err(sink_error("doris sink does not accept raw text input"))
    }

    async fn sink_bytes(&mut self, _data: &[u8]) -> SinkResult<()> {
        Err(sink_error("doris sink does not accept raw byte input"))
    }

    async fn sink_str_batch(&mut self, _data: Vec<&str>) -> SinkResult<()> {
        Err(sink_error("doris sink does not accept raw batch input"))
    }

    async fn sink_bytes_batch(&mut self, _data: Vec<&[u8]>) -> SinkResult<()> {
        Err(sink_error(
            "doris sink does not accept raw batch byte input",
        ))
    }
}

/// 以 MySQL 语法转义标识符，支持 `db.table`。
///
/// # args
/// * `input` - 需要被引用的名称。
///
/// # return
/// * `String` - 每段都以反引号包裹的标识符。
fn quote_identifier(input: &str) -> String {
    input
        .split('.')
        .map(|segment| format!("`{}`", segment.replace('`', "``")))
        .collect::<Vec<_>>()
        .join(".")
}

/// 安全地转义 SQL 字符串值，防止 SQL 注入。
/// 适用于 Doris/MySQL 字符串字面量。
///
/// # args
/// * `value` - 原始字符串。
///
/// # return
/// * `String` - 转义后的字符串，可安全用于 SQL 语句。
fn escape_sql_string(value: &str) -> String {
    value
        .replace('\\', "\\\\") // 反斜杠必须最先处理
        .replace('\'', "''") // 单引号
        .replace('\0', "\\0") // NULL 字节
        .replace('\n', "\\n") // 换行符
        .replace('\r', "\\r") // 回车符
        .replace('\x1a', "\\Z") // Ctrl+Z (Windows EOF)
}

/// 统一封装 sink 层错误，便于上层识别。
///
/// # args
/// * `msg` - 错误描述。
///
/// # return
/// * `SinkError` - 以 `SinkReason::Sink` 包装后的错误。
fn sink_error(msg: impl Into<String>) -> SinkError {
    SinkError::from(SinkReason::Sink(msg.into()))
}

/// 规范化 MySQL 连接参数，禁用不兼容选项并设置账号密码。
///
/// # args
/// * `opts` - 初始的连接配置。
/// * `user`/`password` - 凭证。
///
/// # return
/// * `MySqlConnectOptions` - 可直接用于 sqlx 的配置。
fn sanitize_options(
    mut opts: MySqlConnectOptions,
    user: &str,
    password: &str,
) -> MySqlConnectOptions {
    opts = opts
        .username(user)
        .pipes_as_concat(false)
        .no_engine_substitution(false)
        .set_names(false)
        .timezone(None);
    if !password.is_empty() {
        opts = opts.password(password);
    }
    opts
}

/// 如目标库不存在则创建。
///
/// # args
/// * `cfg` - 当前 sink 配置。
///
/// # return
/// * `anyhow::Result<()>` - 成功后数据库一定存在。
async fn create_database_if_missing(cfg: &DorisSinkConfig) -> anyhow::Result<()> {
    let admin_opts = sanitize_options(
        cfg.endpoint.parse::<MySqlConnectOptions>()?,
        &cfg.user,
        &cfg.password,
    );
    let admin_pool = MySqlPoolOptions::new()
        .max_connections(1)
        .connect_with(admin_opts)
        .await?;
    let create_sql = format!(
        "CREATE DATABASE IF NOT EXISTS {}",
        quote_identifier(&cfg.database)
    );
    raw_sql(&create_sql).execute(&admin_pool).await?;
    Ok(())
}

/// 检查并必要时创建目标表。
/// 注意：Doris 不完全支持预编译语句，使用字符串拼接 + 转义。
///
/// # args
/// * `pool` - 已连接至目标库的连接池。
/// * `database`/`table` - 表所在的库和表名。
/// * `create_stmt` - 可选建表语句模板。
///
/// # return
/// * `anyhow::Result<()>` - 表存在或建表成功。
async fn ensure_table_exists(
    pool: &MySqlPool,
    database: &str,
    table: &str,
    create_stmt: Option<&str>,
) -> anyhow::Result<()> {
    // Doris 不支持预编译，使用字符串拼接但安全转义
    let exists_sql = format!(
        "SELECT COUNT(1) AS cnt FROM information_schema.TABLES WHERE TABLE_SCHEMA='{}' AND TABLE_NAME='{}'",
        escape_sql_string(database),
        escape_sql_string(table)
    );
    let row = raw_sql(&exists_sql).fetch_one(pool).await?;
    let exists: i64 = row.try_get("cnt")?;
    if exists > 0 {
        return Ok(());
    }

    let stmt = create_stmt.ok_or_else(|| {
        anyhow::anyhow!("table `{table}` not found and create_table statement not provided")
    })?;
    let statement = stmt.replace("{table}", table);
    raw_sql(&statement).execute(pool).await?;
    Ok(())
}

/// 从 information_schema 读取列顺序，作为批量写入的列序。
/// 注意：Doris 不完全支持预编译语句，使用字符串拼接 + 转义。
///
/// # args
/// * `pool` - 连接池。
/// * `database`/`table` - 目标表。
///
/// # return
/// * `Vec<String>` - 按 ordinal_position 排序的列名列表。
async fn load_table_columns(
    pool: &MySqlPool,
    database: &str,
    table: &str,
) -> anyhow::Result<Vec<String>> {
    // Doris 不支持预编译，使用字符串拼接但安全转义
    let sql = format!(
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='{}' AND TABLE_NAME='{}' ORDER BY ORDINAL_POSITION",
        escape_sql_string(database),
        escape_sql_string(table)
    );
    let rows = raw_sql(&sql).fetch_all(pool).await?;

    let mut cols = Vec::with_capacity(rows.len());
    for row in rows {
        let name: String = row.try_get("COLUMN_NAME")?;
        cols.push(name);
    }
    Ok(cols)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_identifier_handles_segments() {
        assert_eq!(quote_identifier("events"), "`events`");
        assert_eq!(quote_identifier("demo.events"), "`demo`.`events`");
    }

    #[test]
    fn test_escape_sql_string() {
        // 测试单引号
        assert_eq!(escape_sql_string("O'Reilly"), "O''Reilly");

        // 测试反斜杠
        assert_eq!(escape_sql_string("C:\\path"), "C:\\\\path");

        // 测试换行符
        assert_eq!(escape_sql_string("line1\nline2"), "line1\\nline2");

        // 测试回车符
        assert_eq!(escape_sql_string("text\rmore"), "text\\rmore");

        // 测试 NULL 字节
        assert_eq!(escape_sql_string("text\0null"), "text\\0null");

        // 测试组合
        assert_eq!(
            escape_sql_string("It's a\\path\nwith'quotes"),
            "It''s a\\\\path\\nwith''quotes"
        );
    }
}
