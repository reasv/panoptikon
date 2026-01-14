use anyhow::{Context, Result};
use serde::Deserialize;
use std::{env, path::PathBuf};

#[derive(Debug, Clone, Deserialize)]
pub struct Settings {
    pub server: ServerConfig,
    pub upstreams: UpstreamsConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpstreamsConfig {
    pub ui: String,
    pub api: String,
}

impl Settings {
    pub fn load() -> Result<Self> {
        let config_path = PathBuf::from("gateway").join("config.toml");
        let builder = config::Config::builder()
            .set_default("server.host", "0.0.0.0")?
            .set_default("server.port", 8080)?
            .set_default("upstreams.ui", "http://127.0.0.1:6339")?
            .set_default("upstreams.api", "http://127.0.0.1:6342")?
            .add_source(config::File::from(config_path).required(false))
            .add_source(config::Environment::with_prefix("GATEWAY").separator("__"));

        let mut settings: Settings = builder.build()?.try_deserialize()?;
        settings.apply_env_overrides()?;
        Ok(settings)
    }

    pub fn listen_addr(&self) -> String {
        format!("{}:{}", self.server.host, self.server.port)
    }

    fn apply_env_overrides(&mut self) -> Result<()> {
        if let Ok(value) = env::var("GATEWAY__SERVER_HOST") {
            self.server.host = value;
        }
        if let Ok(value) = env::var("GATEWAY__SERVER_PORT") {
            self.server.port = value
                .parse()
                .context("GATEWAY__SERVER_PORT must be a valid u16")?;
        }
        if let Ok(value) = env::var("GATEWAY__UPSTREAM_UI") {
            self.upstreams.ui = value;
        }
        if let Ok(value) = env::var("GATEWAY__UPSTREAM_API") {
            self.upstreams.api = value;
        }
        Ok(())
    }
}
