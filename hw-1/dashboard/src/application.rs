use reqwest::blocking::Client;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Endpoint {
    pub method: String,
    pub path: String,
}

pub struct Application<'a> {
    client: &'a Client,
    url: String,
}

impl<'a> Application<'a> {
    pub const fn new(client: &'a Client, url: String) -> Self {
        Self { client, url }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn endpoints(&self) -> reqwest::Result<Vec<Endpoint>> {
        self.client
            .get(format!("{}/handlers", self.url))
            .send()?
            .error_for_status()?
            .json()
    }
}
