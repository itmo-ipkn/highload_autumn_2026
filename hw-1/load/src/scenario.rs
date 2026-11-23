use std::sync::Arc;
use std::time::Duration;

use goose::prelude::*;

use crate::item::{ItemIds, NewItem};
use crate::request::{Endpoint, Request};
use crate::targets::Targets;

const WAIT: Duration = Duration::from_secs(1);

pub fn items(targets: &Targets, ids: ItemIds) -> Result<Scenario, GooseError> {
    let scenario = scenario!("Items")
        .set_wait_time(WAIT, WAIT)?
        .register_transaction(transaction(targets, 19, |target| {
            Request::new(Endpoint::Create, format!("{target}/items"), 201)
                .with_body(&NewItem::random())
        })?)
        .register_transaction(transaction(targets, 1, |target| {
            Request::new(Endpoint::Create, format!("{target}/items"), 422)
                .with_body(&NewItem::invalid())
        })?)
        .register_transaction(transaction(targets, 116, move |target| {
            let url = format!("{target}/items/{}", ids.existing());
            Request::new(Endpoint::Get, url, 200)
        })?)
        .register_transaction(transaction(targets, 2, |target| {
            let url = format!("{target}/items/{}", ItemIds::missing());
            Request::new(Endpoint::Get, url, 404)
        })?)
        .register_transaction(transaction(targets, 1, |target| {
            Request::new(Endpoint::Get, format!("{target}/items/not-a-number"), 400)
        })?)
        .register_transaction(transaction(targets, 59, move |target| {
            let url = format!("{target}/items/{}", ids.existing());
            Request::new(Endpoint::Update, url, 200).with_body(&NewItem::random())
        })?)
        .register_transaction(transaction(targets, 2, |target| {
            let url = format!("{target}/items/{}", ItemIds::missing());
            Request::new(Endpoint::Update, url, 404).with_body(&NewItem::random())
        })?);
    Ok(scenario)
}

fn transaction<F>(targets: &Targets, weight: usize, plan: F) -> Result<Transaction, GooseError>
where
    F: Fn(&str) -> Request + Send + Sync + 'static,
{
    let targets = targets.clone();
    let function: TransactionFunction = Arc::new(move |user| {
        let request = plan(targets.random());
        Box::pin(request.send(user))
    });
    Transaction::new(function).set_weight(weight)
}
