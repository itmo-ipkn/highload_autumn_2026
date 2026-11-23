use std::sync::Arc;

use rand::RngExt;

#[derive(Debug, Clone)]
pub struct Targets(Arc<[String]>);

impl Targets {
    pub fn parse(list: &str) -> Self {
        Self(
            list.split(',')
                .map(|target| target.trim().trim_end_matches('/').to_owned())
                .collect(),
        )
    }

    pub fn first(&self) -> &str {
        &self.0[0]
    }

    pub fn random(&self) -> &str {
        &self.0[rand::rng().random_range(0..self.0.len())]
    }

    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.0.iter().map(String::as_str)
    }
}
