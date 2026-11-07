package org.chistyakov.dashboard;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.concurrent.Callable;

public class Generate {
    static final ObjectMapper M = new ObjectMapper();
    static final HttpClient H = HttpClient.newHttpClient();
    static final String APP = System.getenv("APP_URL"), GRAFANA = System.getenv("GRAFANA_URL");
    static final String AUTH = "Basic " + Base64.getEncoder().encodeToString(
            (System.getenv("GRAFANA_ADMIN_USER") + ":" + System.getenv("GRAFANA_ADMIN_PASSWORD")).getBytes());
    static final Map<String, String> DS = Map.of("type", "prometheus",
            "uid", System.getenv().getOrDefault("DATASOURCE_UID", "prometheus"));
    static final String MT = "http_server_requests_seconds";

    static final String LATENCY = "histogram_quantile(%s, sum by (le) (rate(" + MT + "_bucket{%s}[1m])))";
    static final String RPS = "sum(rate(" + MT + "_count{%s}[1m]))";
    static final String ERRORS = "sum by (status) (rate(" + MT + "_count{%s,status=~\"4..|5..\"}[1m]))";

    static Map<String, Object> o(Object... kv) {
        var m = new LinkedHashMap<String, Object>();
        for (int i = 0; i < kv.length; i += 2) m.put((String) kv[i], kv[i + 1]);
        return m;
    }

    static <T> T retry(Callable<T> f) throws Exception {
        for (int i = 1; ; i++) {
            try {
                return f.call();
            } catch (Exception e) {
                if (i == 30) throw e;
                System.out.printf("[%d/30] %s%n", i, e.getMessage());
                Thread.sleep(2000);
            }
        }
    }

    static String call(HttpRequest.Builder b) throws Exception {
        var r = H.send(b.build(), HttpResponse.BodyHandlers.ofString());
        if (r.statusCode() != 200) throw new IllegalStateException(r.statusCode() + " " + r.body());
        return r.body();
    }

    static Map<String, Object> panel(int id, String title, int x, int y, String unit, String... q) {
        var targets = new ArrayList<>();
        for (int i = 0; i < q.length; i += 2)
            targets.add(o("datasource", DS, "refId", String.valueOf((char) ('A' + i / 2)),
                    "expr", q[i], "legendFormat", q[i + 1]));
        return o("id", id, "type", "timeseries", "title", title, "datasource", DS, "targets", targets,
                "gridPos", o("x", x, "y", y, "w", 8, "h", 8),
                "fieldConfig", o("defaults", o("unit", unit), "overrides", List.of()));
    }

    static Map<String, Object> variable(String name, String query) {
        return o("name", name, "label", name, "type", "query", "datasource", DS,
                "query", o("query", query, "refId", name), "definition", query,
                "refresh", 2, "multi", true, "includeAll", true, "allValue", ".*",
                "current", o("selected", true, "text", List.of("All"), "value", List.of("$__all")));
    }

    public static void main(String[] args) throws Exception {
        JsonNode handlers = M.readTree(retry(() -> call(HttpRequest.newBuilder(URI.create(APP + "/handlers")))));
        System.out.println("handlers: " + handlers);

        var panels = new ArrayList<>();
        int i = 0;
        for (JsonNode h : handlers) {
            int y = i * 9, id = i++ * 10;
            String name = h.get("method").asText() + " " + h.get("path").asText();
            String s = "method=\"%s\",uri=\"%s\",datacenter=~\"$datacenter\",pod_id=~\"$pod_id\""
                    .formatted(h.get("method").asText(), h.get("path").asText());
            panels.add(o("id", id, "type", "row", "title", name, "collapsed", false, "panels", List.of(),
                    "gridPos", o("x", 0, "y", y, "w", 24, "h", 1)));
            panels.add(panel(id + 1, name + ": latency", 0, y + 1, "s",
                    LATENCY.formatted("0.5", s), "p50",
                    LATENCY.formatted("0.95", s), "p95",
                    LATENCY.formatted("0.99", s), "p99"));
            panels.add(panel(id + 2, name + ": RPS", 8, y + 1, "reqps", RPS.formatted(s), "rps"));
            panels.add(panel(id + 3, name + ": errors", 16, y + 1, "reqps", ERRORS.formatted(s), "{{status}}"));
        }

        var dashboard = o("uid", "app", "title", "app", "editable", false,
                "refresh", "5s", "time", o("from", "now-15m", "to", "now"),
                "templating", o("list", List.of(
                        variable("datacenter", "label_values(" + MT + "_count, datacenter)"),
                        variable("pod_id", "label_values(" + MT + "_count{datacenter=~\"$datacenter\"}, pod_id)"))),
                "panels", panels);
        String body = M.writeValueAsString(o("dashboard", dashboard, "overwrite", true));

        String res = retry(() -> call(HttpRequest.newBuilder(URI.create(GRAFANA + "/api/dashboards/db"))
                .header("Content-Type", "application/json").header("Authorization", AUTH)
                .POST(HttpRequest.BodyPublishers.ofString(body))));
        System.out.println("dashboard created");
    }
}