//! 时间统计工具
//!
//! 提供通用的批处理时间统计功能，用于记录：
//! - 批次数量统计
//! - 执行时间统计
//! - 间隔时间统计

use std::time::{Duration, Instant};

/// 时间统计工具
pub struct TimeStatUtils {
    /// 累计处理的总数量
    cnt: u64,
    /// 上次运行结束时间
    last_end_time: Option<Instant>,
    /// 运行总时间
    total_duration: Duration,
    /// 总间隔时长
    total_interval: Duration,
    /// 当前批次开始时间
    current_start_time: Option<Instant>,
    /// 当前批次数量
    current_count: u64,
    /// 当前批次间隔时间
    current_interval: Duration,
}

impl TimeStatUtils {
    /// 创建新的时间统计实例
    pub fn new() -> Self {
        Self {
            cnt: 0,
            last_end_time: None,
            total_duration: Duration::ZERO,
            total_interval: Duration::ZERO,
            current_start_time: None,
            current_count: 0,
            current_interval: Duration::ZERO,
        }
    }

    /// 开始统计
    ///
    /// # Arguments
    /// * `count` - 当前批次的数量
    pub fn start_stat(&mut self, count: u64) {
        let start_time = Instant::now();

        // 计算距离上次的间隔
        self.current_interval = if let Some(last_end) = self.last_end_time {
            let interval = start_time.duration_since(last_end);
            self.total_interval += interval;
            interval
        } else {
            Duration::ZERO
        };

        self.current_start_time = Some(start_time);
        self.current_count = count;
        self.cnt += count;
    }

    /// 结束统计
    pub fn end_stat(&mut self) {
        let end_time = Instant::now();

        if let Some(start_time) = self.current_start_time {
            let execution_time = end_time.duration_since(start_time);
            self.total_duration += execution_time;
        }

        self.last_end_time = Some(end_time);
    }

    /// 打印统计信息
    ///
    /// # Arguments
    /// * `key` - 标识符（如 "DorisSink-0"）
    pub fn println(&self, key: &str) {
        let execution_time = if let (Some(start_time), Some(end_time)) =
            (self.current_start_time, self.last_end_time)
        {
            end_time.duration_since(start_time)
        } else {
            Duration::ZERO
        };

        // 获取当前时间戳（毫秒）
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);

        wp_log::info_mtrc!(
            "[{}][{}] 统计 - 当前: {} 条, 总计: {} 条, 间隔: {:.3}s, 本次耗时: {:.3}s, 总耗时: {:.3}s, 总间隔: {:.3}s",
            timestamp,
            key,
            self.current_count,
            self.cnt,
            self.current_interval.as_secs_f64(),
            execution_time.as_secs_f64(),
            self.total_duration.as_secs_f64(),
            self.total_interval.as_secs_f64()
        );
    }
}

impl Default for TimeStatUtils {
    fn default() -> Self {
        Self::new()
    }
}
