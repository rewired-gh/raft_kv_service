use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    time::Duration,
};

use anyhow::{Context, Result};
use raft_kv_service::{config::AppConfig, telemetry};

#[tokio::main]
async fn main() -> Result<()> {
    if let Some(address) = healthcheck_argument()? {
        return healthcheck(address);
    }
    let config = AppConfig::load()?;
    telemetry::init(&config.telemetry)?;
    raft_kv_service::run(config).await
}

fn healthcheck_argument() -> Result<Option<SocketAddr>> {
    let mut arguments = std::env::args().skip(1);
    let Some(command) = arguments.next() else {
        return Ok(None);
    };
    if command != "healthcheck" {
        anyhow::bail!("unknown command {command:?}");
    }
    let address = arguments
        .next()
        .context("healthcheck requires HOST:PORT")?
        .parse()
        .context("invalid healthcheck address")?;
    if arguments.next().is_some() {
        anyhow::bail!("healthcheck accepts exactly one HOST:PORT");
    }
    Ok(Some(address))
}

fn healthcheck(address: SocketAddr) -> Result<()> {
    let timeout = Duration::from_secs(2);
    let mut stream = TcpStream::connect_timeout(&address, timeout)
        .with_context(|| format!("cannot connect to {address}"))?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    stream
        .write_all(b"GET /health/live HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")?;
    let mut response = [0_u8; 32];
    let length = stream.read(&mut response)?;
    let healthy = response[..length].starts_with(b"HTTP/1.1 200")
        || response[..length].starts_with(b"HTTP/1.0 200");
    if !healthy {
        anyhow::bail!("liveness endpoint did not return HTTP 200");
    }
    Ok(())
}
