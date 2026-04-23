#![allow(dead_code)]

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use wp_connector_api::{ParamMap, SourceEvent, SourceFactory};

/// 将任何可序列化对象转换为 ParamMap。
#[allow(dead_code)]
pub fn to_param_map<T: Serialize>(obj: &T) -> Result<ParamMap> {
    let value = serde_json::to_value(obj)?;
    match value {
        Value::Object(map) => Ok(map.into_iter().collect()),
        _ => anyhow::bail!("Expected an object, got {:?}", value),
    }
}

/// 将 ParamMap 转换为指定类型的对象。
#[allow(dead_code)]
pub fn from_param_map<T: for<'de> Deserialize<'de>>(params: &ParamMap) -> Result<T> {
    let value = Value::Object(params.clone().into_iter().collect());
    let obj = serde_json::from_value(value)?;
    Ok(obj)
}

pub type AsyncInitFn =
    Box<dyn Fn() -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync>;

pub type AsyncWaitReadyFn =
    Box<dyn Fn(ParamMap) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync>;

pub type AsyncInputFn =
    Box<dyn Fn(ParamMap) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync>;

pub type AsyncAssertFn =
    Box<dyn Fn(SourceRunContext) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceRunPhase {
    Initial,
    AfterRestart,
}

#[derive(Clone, Copy, Debug)]
pub struct SourceCollectConfig {
    pub timeout: Duration,
    pub poll_interval: Duration,
}

impl Default for SourceCollectConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(5),
            poll_interval: Duration::from_millis(100),
        }
    }
}

pub struct SourceRunContext {
    pub display_name: String,
    pub params: ParamMap,
    pub phase: SourceRunPhase,
    pub source_count: usize,
    pub receive_attempts: usize,
    pub idle_count: usize,
    pub eof_count: usize,
    pub elapsed: Duration,
    pub received_events: Vec<SourceEvent>,
}

/// Source 集成测试信息。
pub struct SourceInfo<F: SourceFactory> {
    factory: F,
    test_name: Option<String>,
    params: ParamMap,
    tags: Vec<String>,
    init_fn: Option<AsyncInitFn>,
    wait_ready_fn: Option<AsyncWaitReadyFn>,
    input_fn: Option<AsyncInputFn>,
    assert_fn: Option<AsyncAssertFn>,
    collect_config: SourceCollectConfig,
    restart_verification: bool,
}

impl<F: SourceFactory> SourceInfo<F> {
    pub fn new(factory: F, params: ParamMap) -> Self {
        Self {
            factory,
            test_name: None,
            params,
            tags: Vec::new(),
            init_fn: None,
            wait_ready_fn: None,
            input_fn: None,
            assert_fn: None,
            collect_config: SourceCollectConfig::default(),
            restart_verification: false,
        }
    }

    pub fn with_test_name(mut self, test_name: impl Into<String>) -> Self {
        self.test_name = Some(test_name.into());
        self
    }

    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }

    pub fn with_async_init<Fut>(mut self, init_fn: impl Fn() -> Fut + Send + Sync + 'static) -> Self
    where
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.init_fn = Some(Box::new(move || Box::pin(init_fn())));
        self
    }

    pub fn with_async_wait_ready<Fut>(
        mut self,
        wait_ready_fn: impl Fn(ParamMap) -> Fut + Send + Sync + 'static,
    ) -> Self
    where
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.wait_ready_fn = Some(Box::new(move |params| Box::pin(wait_ready_fn(params))));
        self
    }

    pub fn with_async_input<Fut>(
        mut self,
        input_fn: impl Fn(ParamMap) -> Fut + Send + Sync + 'static,
    ) -> Self
    where
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.input_fn = Some(Box::new(move |params| Box::pin(input_fn(params))));
        self
    }

    pub fn with_async_assert<Fut>(
        mut self,
        assert_fn: impl Fn(SourceRunContext) -> Fut + Send + Sync + 'static,
    ) -> Self
    where
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.assert_fn = Some(Box::new(move |ctx| Box::pin(assert_fn(ctx))));
        self
    }

    pub fn with_collect_timeout(mut self, timeout: Duration) -> Self {
        self.collect_config.timeout = timeout;
        self
    }

    pub fn with_poll_interval(mut self, poll_interval: Duration) -> Self {
        self.collect_config.poll_interval = poll_interval;
        self
    }

    pub fn with_restart_verification(mut self, enabled: bool) -> Self {
        self.restart_verification = enabled;
        self
    }

    pub fn factory(&self) -> &F {
        &self.factory
    }

    pub fn params(&self) -> &ParamMap {
        &self.params
    }

    pub fn tags(&self) -> &[String] {
        &self.tags
    }

    pub fn test_name(&self) -> Option<&str> {
        self.test_name.as_deref()
    }

    pub fn collect_config(&self) -> SourceCollectConfig {
        self.collect_config
    }

    pub fn restart_verification(&self) -> bool {
        self.restart_verification
    }

    pub async fn init(&self) -> Result<()> {
        if let Some(init_fn) = &self.init_fn {
            init_fn().await?;
        }
        Ok(())
    }

    pub async fn wait_ready(&self) -> Result<()> {
        if let Some(wait_ready_fn) = &self.wait_ready_fn {
            wait_ready_fn(self.params.clone()).await?;
        }
        Ok(())
    }

    pub async fn input(&self) -> Result<()> {
        if let Some(input_fn) = &self.input_fn {
            input_fn(self.params.clone()).await?;
        }
        Ok(())
    }

    pub async fn assert(&self, ctx: SourceRunContext) -> Result<()> {
        if let Some(assert_fn) = &self.assert_fn {
            assert_fn(ctx).await?;
        }
        Ok(())
    }
}
