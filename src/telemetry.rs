use anyhow::{Context, Result};
use tracing_subscriber::EnvFilter;

use crate::config::{LogFormat, TelemetryConfig};

pub fn init(config: &TelemetryConfig) -> Result<()> {
    let filter = EnvFilter::try_new(&config.log_filter)
        .context("telemetry.log_filter is not a valid tracing filter")?;

    let result = match config.log_format {
        LogFormat::Json => tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .with_current_span(true)
            .with_span_list(true)
            .try_init(),
        LogFormat::Pretty => tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(true)
            .try_init(),
    };

    result.map_err(|error| anyhow::anyhow!("failed to initialize tracing subscriber: {error}"))
}
