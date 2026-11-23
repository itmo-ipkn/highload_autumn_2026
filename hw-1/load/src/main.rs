mod config;
mod item;
mod request;
mod scenario;
mod seed;
mod targets;

use std::error::Error;

use goose::prelude::*;

use crate::config::Config;
use crate::item::ItemIds;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::from_env();

    seed::run(&reqwest::Client::new(), &config.targets, config.seed_items).await?;

    let ids = ItemIds::new(config.seed_items);
    GooseAttack::initialize()?
        .register_scenario(scenario::items(&config.targets, ids)?)
        .set_scheduler(GooseScheduler::Random)
        .set_default(GooseDefault::Host, config.targets.first())?
        .set_default(GooseDefault::Users, config.target_rps)?
        .set_default(GooseDefault::IncreaseTime, config.ramp_up_secs)?
        .set_default(GooseDefault::NoTelnet, true)?
        .execute()
        .await?;

    Ok(())
}
