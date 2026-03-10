use educe::Educe;
use serde::{Deserialize, Serialize};
use winnow::error::ModalResult;
use winnow::prelude::*;
use winnow::token::{literal, take_till, take_until};

/// 与 `structure::io::Postgres` 等价的配置结构，集中到 postgres/ 目录便于后续抽离。
#[derive(Educe, Deserialize, Serialize, PartialEq, Clone)]
#[educe(Debug, Default)]
pub struct PostgresConf {
    /// 形如 `host:port` 的地址
    pub endpoint: String,
    #[educe(Default = "root")]
    pub username: String,
    #[educe(Default = "dayu")]
    pub password: String,
    #[educe(Default = "wparse")]
    pub database: String,
    pub table: Option<String>,
    /// 批量写入的条数（可选）
    pub batch: Option<usize>,
}

impl PostgresConf {
    /// 优先读取环境变量 `POSTGRES_URL`，否则拼接配置字段。
    /// postgres://root:root@localhost:3306/database
    pub fn get_database_url(&self) -> String {
        if let Ok(url) = std::env::var("POSTGRES_URL") {
            url
        } else {
            format!(
                "postgres://{}:{}@{}/{}",
                self.username, self.password, self.endpoint, self.database
            )
        }
    }

    /// 从标准连接串解析 Postgres 配置。
    /// 支持：postgres://<user>:<pass>@<host[:port]>/<db>[?params]
    pub fn parse_postgres_connect(input: &mut &str) -> ModalResult<PostgresConf> {
        // 解析形如：postgres://user:pass@host[:port]/database[?params]
        literal("postgres://").parse_next(input)?;
        let username: &str = take_until(0.., ":").parse_next(input)?;
        literal(":").parse_next(input)?;
        let password: &str = take_until(0.., "@").parse_next(input)?;
        literal("@").parse_next(input)?;
        let address: &str = take_until(0.., "/").parse_next(input)?;
        literal("/").parse_next(input)?;
        // 数据库名读到 `?`（若无参数则读到结尾）
        let database: &str = take_till(0.., |c: char| c == '?').parse_next(input)?;

        Ok(PostgresConf {
            endpoint: address.to_string(),
            username: username.to_string(),
            password: password.to_string(),
            database: database.to_string(),
            table: None,
            batch: None,
        })
    }

    /// 便捷封装：从 url 解析并返回结构体。
    pub fn from_url(url: &str) -> anyhow::Result<Self> {
        let mut s = url;
        Self::parse_postgres_connect(&mut s)
            .map_err(|e| anyhow::anyhow!("parse postgres url failed: {:?}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::PostgresConf;

    #[test]
    fn test_parse_postgres_url() {
        let url = "postgres://root:dayu@localhost:3306/wparse";
        let mut s = url;
        let cfg = PostgresConf::parse_postgres_connect(&mut s).unwrap();
        assert_eq!(s, "");
        assert_eq!(cfg.endpoint, "localhost:3306");
        assert_eq!(cfg.username, "root");
        assert_eq!(cfg.password, "dayu");
        assert_eq!(cfg.database, "wparse");
    }
}
