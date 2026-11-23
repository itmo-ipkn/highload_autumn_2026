mod application;
mod config;
mod dashboard;
mod grafana;
mod panel;

use std::error::Error;

use reqwest::blocking::Client;

use crate::application::Application;
use crate::config::Config;
use crate::dashboard::Datasource;
use crate::grafana::Grafana;

fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::from_env()?;
    let client = Client::new();

    let application = Application::new(&client, config.app_url);
    let endpoints = application.endpoints()?;
    println!("found {} endpoints at {}", endpoints.len(), application.url());

    let dashboard = dashboard::build(&endpoints, &Datasource::new(config.datasource_uid));

    let grafana = Grafana::new(&client, config.grafana_url, config.grafana_credentials);
    let url = grafana.upload(&dashboard)?;
    println!("dashboard uploaded: {url}");

    Ok(())
}
