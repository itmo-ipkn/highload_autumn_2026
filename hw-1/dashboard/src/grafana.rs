use reqwest::blocking::Client;
use serde::Deserialize;
use serde_json::{Value, json};

pub struct Credentials {
    pub user: String,
    pub password: String,
}

pub struct Grafana<'a> {
    client: &'a Client,
    url: String,
    credentials: Credentials,
}

#[derive(Deserialize)]
struct UploadResponse {
    url: String,
}

impl<'a> Grafana<'a> {
    pub const fn new(client: &'a Client, url: String, credentials: Credentials) -> Self {
        Self {
            client,
            url,
            credentials,
        }
    }

    pub fn upload(&self, dashboard: &Value) -> reqwest::Result<String> {
        let response: UploadResponse = self
            .client
            .post(format!("{}/api/dashboards/db", self.url))
            .basic_auth(&self.credentials.user, Some(&self.credentials.password))
            .json(&json!({
                "dashboard": dashboard,
                "overwrite": true,
                "message": "generated from /handlers",
            }))
            .send()?
            .error_for_status()?
            .json()?;
        Ok(format!("{}{}", self.url, response.url))
    }
}
