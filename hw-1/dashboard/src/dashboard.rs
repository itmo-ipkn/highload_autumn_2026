use serde_json::{Value, json};

use crate::application::Endpoint;
use crate::panel::{self, GridPosition};

const UID: &str = "simple-crud";
const TITLE: &str = "simple-crud: golden signals";
const COLUMNS: u32 = 3;
const PANEL_WIDTH: u32 = 8;
const PANEL_HEIGHT: u32 = 8;

pub struct Datasource(String);

impl Datasource {
    pub const fn new(uid: String) -> Self {
        Self(uid)
    }

    pub fn to_json(&self) -> Value {
        json!({ "type": "prometheus", "uid": self.0 })
    }
}

pub fn build(endpoints: &[Endpoint], datasource: &Datasource) -> Value {
    let mut panels = Vec::new();
    let mut ids = 1..;
    let mut y = 0;

    for endpoint in endpoints {
        panels.push(panel::row(
            ids.next(),
            &format!("{} {}", endpoint.method, endpoint.path),
            GridPosition::new(0, y, COLUMNS * PANEL_WIDTH, 1),
        ));
        y += 1;

        let row = [panel::latency, panel::rps, panel::errors];
        for (column, build) in (0..).zip(row) {
            let mut panel = build(endpoint, datasource);
            panel["id"] = json!(ids.next());
            panel["gridPos"] =
                GridPosition::new(column * PANEL_WIDTH, y, PANEL_WIDTH, PANEL_HEIGHT).to_json();
            panels.push(panel);
        }
        y += PANEL_HEIGHT;
    }

    json!({
        "id": null,
        "uid": UID,
        "title": TITLE,
        "tags": ["simple-crud", "generated"],
        "editable": false,
        "timezone": "browser",
        "refresh": "10s",
        "time": { "from": "now-15m", "to": "now" },
        "schemaVersion": 39,
        "templating": {
            "list": [
                variable(
                    "datacenter",
                    "Datacenter",
                    &format!("label_values({}, datacenter)", panel::REQUESTS_TOTAL),
                    datasource,
                ),
                variable(
                    "pod",
                    "Pod",
                    &format!(
                        "label_values({}{{datacenter=~\"$datacenter\"}}, pod)",
                        panel::REQUESTS_TOTAL
                    ),
                    datasource,
                ),
            ],
        },
        "panels": panels,
    })
}

fn variable(name: &str, label: &str, query: &str, datasource: &Datasource) -> Value {
    json!({
        "name": name,
        "label": label,
        "type": "query",
        "datasource": datasource.to_json(),
        "definition": query,
        "query": { "query": query, "refId": name },
        "refresh": 2,
        "sort": 1,
        "multi": true,
        "includeAll": true,
        "allValue": ".*",
        "current": { "text": ["All"], "value": ["$__all"] },
    })
}
