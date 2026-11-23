use std::env;
use std::error::Error;

use crate::grafana::Credentials;

pub struct Config {
    pub app_url: String,
    pub grafana_url: String,
    pub grafana_credentials: Credentials,
    pub datasource_uid: String,
}

impl Config {
    pub fn from_env() -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            app_url: required("APP_URL")?,
            grafana_url: required("GRAFANA_URL")?,
            grafana_credentials: Credentials {
                user: required("GRAFANA_ADMIN_USER")?,
                password: required("GRAFANA_ADMIN_PASSWORD")?,
            },
            datasource_uid: env::var("DATASOURCE_UID").unwrap_or_else(|_| "prometheus".to_owned()),
        })
    }
}

fn required(name: &str) -> Result<String, Box<dyn Error>> {
    env::var(name).map_err(|_| format!("environment variable {name} must be set").into())
}
