use axum::Router;
use axum::handler::Handler;
use axum::http::Method;
use axum::routing::{MethodFilter, on};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct HandlerInfo {
    pub method: String,
    pub path: String,
}

pub struct Registry<S> {
    router: Router<S>,
    handlers: Vec<HandlerInfo>,
}

impl<S> Default for Registry<S>
where
    S: Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self {
            router: Router::new(),
            handlers: Vec::new(),
        }
    }
}

impl<S> Registry<S>
where
    S: Clone + Send + Sync + 'static,
{
    #[must_use]
    pub fn route<H, T>(mut self, method: Method, path: &str, handler: H) -> Self
    where
        H: Handler<T, S>,
        T: 'static,
    {
        self.handlers.push(HandlerInfo {
            method: method.to_string(),
            path: path.to_owned(),
        });
        let filter = MethodFilter::try_from(method).expect("unsupported HTTP method");
        self.router = self.router.route(path, on(filter, handler));
        self
    }

    pub fn into_parts(self) -> (Router<S>, Vec<HandlerInfo>) {
        (self.router, self.handlers)
    }
}
