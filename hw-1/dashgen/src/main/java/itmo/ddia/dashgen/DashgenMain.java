package itmo.ddia.dashgen;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.List;

import lombok.extern.slf4j.Slf4j;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

@Slf4j
public final class DashgenMain {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(TIMEOUT).build();
    private static final JsonMapper JSON = JsonMapper.builder().build();

    private DashgenMain() {
    }

    public static void main(String[] args) {
        try {
            run();
        } catch (Exception e) {
            log.error("Dashboard generation failed", e);
            System.exit(1);
        }
    }

    private static void run() throws IOException, InterruptedException {
        String appUrl = env("APP_URL", "http://localhost:8080");
        String grafanaUrl = env("GRAFANA_URL", "http://localhost:3000");
        String credentials = env("GRAFANA_ADMIN_USER", null) + ":" + env("GRAFANA_ADMIN_PASSWORD", null);

        HttpRequest handlersRequest = HttpRequest.newBuilder(URI.create(appUrl + "/handlers"))
                .timeout(TIMEOUT)
                .GET()
                .build();
        List<Handler> handlers = JSON.readValue(send(handlersRequest), new TypeReference<>() {
        });
        log.info("Fetched {} handlers from {}", handlers.size(), appUrl);
        handlers.forEach(handler -> log.info("Handler {} {}", handler.method(), handler.path()));

        ObjectNode dashboard = JSON.createObjectNode();
        dashboard.put("uid", "hw1-golden-signals");
        dashboard.put("title", "hw-1 golden signals");
        dashboard.putArray("panels");

        ObjectNode body = JSON.createObjectNode();
        body.set("dashboard", dashboard);
        body.put("overwrite", true);
        HttpRequest saveRequest = HttpRequest.newBuilder(URI.create(grafanaUrl + "/api/dashboards/db"))
                .timeout(TIMEOUT)
                .header("Authorization", "Basic " + Base64.getEncoder()
                        .encodeToString(credentials.getBytes(StandardCharsets.UTF_8)))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)))
                .build();
        String saved = send(saveRequest);
        log.info("Dashboard saved at {}{}", grafanaUrl, JSON.readTree(saved).path("url").asString());
    }

    private static String send(HttpRequest request) throws IOException, InterruptedException {
        HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IllegalStateException(request.method() + " " + request.uri()
                    + " returned " + response.statusCode() + ": " + response.body());
        }

        return response.body();
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        if (value != null && !value.isBlank()) {
            return value;
        }
        if (defaultValue == null) {
            throw new IllegalStateException("Environment variable " + name + " is required");
        }

        return defaultValue;
    }
}
