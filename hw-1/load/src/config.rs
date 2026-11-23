use std::env;
use std::str::FromStr;

use crate::targets::Targets;

pub struct Config {
    pub targets: Targets,
    pub target_rps: usize,
    pub ramp_up_secs: usize,
    pub seed_items: u32,
}

impl Config {
    pub fn from_env() -> Self {
        let targets = env::var("TARGETS").unwrap_or_else(|_| "http://localhost:3000".to_owned());
        Self {
            targets: Targets::parse(&targets),
            target_rps: env_or("TARGET_RPS", 40),
            ramp_up_secs: env_or("RAMP_UP_SECS", 180),
            seed_items: env_or("SEED_ITEMS", 100),
        }
    }
}

fn env_or<T: FromStr>(name: &str, default: T) -> T {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
