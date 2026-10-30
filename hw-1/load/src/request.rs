use goose::prelude::*;
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Endpoint {
    Create,
    Get,
    Update,
}

impl Endpoint {
    pub const ALL: [Self; 3] = [Self::Create, Self::Get, Self::Update];

    pub const fn method(self) -> &'static str {
        match self {
            Self::Create => "POST",
            Self::Get => "GET",
            Self::Update => "PUT",
        }
    }

    pub const fn path(self) -> &'static str {
        match self {
            Self::Create => "/items",
            Self::Get | Self::Update => "/items/{id}",
        }
    }

    const fn goose_method(self) -> GooseMethod {
        match self {
            Self::Create => GooseMethod::Post,
            Self::Get => GooseMethod::Get,
            Self::Update => GooseMethod::Put,
        }
    }

    fn name(self) -> String {
        format!("{} {}", self.method(), self.path())
    }
}

pub struct Request {
    endpoint: Endpoint,
    url: String,
    body: Option<Value>,
    expected_status: u16,
}

impl Request {
    pub const fn new(endpoint: Endpoint, url: String, expected_status: u16) -> Self {
        Self {
            endpoint,
            url,
            body: None,
            expected_status,
        }
    }

    pub fn with_body(self, body: &impl Serialize) -> Self {
        Self {
            body: Some(serde_json::to_value(body).expect("request body serializes to JSON")),
            ..self
        }
    }

    pub async fn send(self, user: &mut GooseUser) -> TransactionResult {
        let method = self.endpoint.goose_method();
        let mut builder = user.get_request_builder(&method, &self.url)?;
        if let Some(body) = &self.body {
            builder = builder.json(body);
        }
        let name = self.endpoint.name();
        let request = GooseRequest::builder()
            .method(method)
            .path(self.url.as_str())
            .name(name.as_str())
            .expect_status_code(self.expected_status)
            .set_request_builder(builder)
            .build();
        user.request(request).await?;
        Ok(())
    }
}
