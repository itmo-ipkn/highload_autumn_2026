use reqwest::Client;
use serde::Deserialize;

use crate::item::NewItem;
use crate::request::Endpoint;
use crate::targets::Targets;

const SEED_ATTEMPTS: u32 = 5;

#[derive(Debug, Deserialize)]
struct Handler {
    method: String,
    path: String,
}

impl Handler {
    fn is_covered(&self) -> bool {
        Endpoint::ALL
            .iter()
            .any(|endpoint| endpoint.method() == self.method && endpoint.path() == self.path)
    }
}

pub async fn run(client: &Client, targets: &Targets, items: u32) -> reqwest::Result<()> {
    for target in targets.iter() {
        let handlers: Vec<Handler> = client
            .get(format!("{target}/handlers"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        for handler in handlers.iter().filter(|handler| !handler.is_covered()) {
            eprintln!(
                "{target}: endpoint {} {} is not covered by the load test",
                handler.method, handler.path
            );
        }

        for _ in 0..items {
            create_item(client, target, &NewItem::random()).await?;
        }
        println!("{target}: seeded {items} items");
    }
    Ok(())
}

async fn create_item(client: &Client, target: &str, item: &NewItem) -> reqwest::Result<()> {
    let mut attempt = 1;
    loop {
        let response = client
            .post(format!("{target}/items"))
            .json(item)
            .send()
            .await?;
        match response.error_for_status() {
            Ok(_) => return Ok(()),
            Err(error)
                if attempt < SEED_ATTEMPTS
                    && error.status().is_some_and(|status| status.is_server_error()) =>
            {
                eprintln!("{target}: seeding attempt {attempt} failed ({error}), retrying");
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
}
