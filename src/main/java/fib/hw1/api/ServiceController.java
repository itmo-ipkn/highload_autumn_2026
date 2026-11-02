package fib.hw1.api;

import fib.hw1.metrics.HandlerCatalog;
import fib.hw1.metrics.HandlerDescriptor;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
public class ServiceController {
    private final PrometheusMeterRegistry registry;
    private final HandlerCatalog handlerCatalog;

    public ServiceController(PrometheusMeterRegistry registry, HandlerCatalog handlerCatalog) {
        this.registry = registry;
        this.handlerCatalog = handlerCatalog;
    }

    @GetMapping("/health")
    public Map<String, String> health() {
        return Map.of("status", "UP");
    }

    @GetMapping(value = "/metrics", produces = "text/plain; version=0.0.4; charset=utf-8")
    public String metrics() {
        return registry.scrape();
    }

    @GetMapping("/handlers")
    public List<HandlerDescriptor> handlers() {
        return handlerCatalog.list();
    }
}
