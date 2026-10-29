package itmo.ddia.dashgen;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ArrayNode;
import tools.jackson.databind.node.ObjectNode;

final class DashboardTemplate {

    private static final int HANDLER_HEIGHT = 9;

    private DashboardTemplate() {
    }

    static ObjectNode render(JsonMapper json, List<Handler> handlers) {
        ObjectNode dashboard = (ObjectNode) json.readTree(resource("dashboard.json"));
        String handlerPanels = resource("handler-panels.json");

        ArrayNode panels = (ArrayNode) dashboard.get("panels");
        int id = 1;
        for (int i = 0; i < handlers.size(); i++) {
            String filled = handlerPanels
                    .replace("__METHOD__", handlers.get(i).method())
                    .replace("__PATH__", handlers.get(i).path());
            for (JsonNode node : json.readTree(filled)) {
                ObjectNode panel = (ObjectNode) node;
                panel.put("id", id++);
                ObjectNode gridPos = (ObjectNode) panel.get("gridPos");
                gridPos.put("y", gridPos.get("y").asInt() + i * HANDLER_HEIGHT);
                panels.add(panel);
            }
        }

        return dashboard;
    }

    private static String resource(String name) {
        try (InputStream in = DashboardTemplate.class.getResourceAsStream("/" + name)) {
            return new String(Objects.requireNonNull(in, "Missing resource " + name).readAllBytes(),
                    StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
