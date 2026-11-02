package fib.hw1.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.servlet.HandlerMapping;

import java.io.IOException;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Component
@Order(Ordered.LOWEST_PRECEDENCE)
public class HttpRequestMetricsFilter extends OncePerRequestFilter {
    private static final Set<String> EXCLUDED_ROUTES = Set.of("/health", "/metrics", "/handlers");

    private final PrometheusMeterRegistry registry;
    private final String podId;
    private final String datacenter;

    public HttpRequestMetricsFilter(
            PrometheusMeterRegistry registry,
            @Value("${app.pod-id:unknown}") String podId,
            @Value("${app.datacenter:unknown}") String datacenter) {
        this.registry = registry;
        this.podId = podId;
        this.datacenter = datacenter;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        long startedAt = System.nanoTime();
        try {
            filterChain.doFilter(request, response);
        } finally {
            Object matchedPattern = request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);
            if (matchedPattern instanceof String route && !EXCLUDED_ROUTES.contains(route)) {
                int status = response.getStatus();
                String method = request.getMethod();
                String statusTag = Integer.toString(status);

                Counter.builder("http.requests")
                        .description("Total number of HTTP requests handled by business endpoints")
                        .tags("pod", podId,
                                "datacenter", datacenter,
                                "method", method,
                                "route", route,
                                "status", statusTag)
                        .register(registry)
                        .increment();

                Timer.builder("http.server.request.duration")
                        .description("HTTP request latency for business endpoints")
                        .tags("pod", podId,
                                "datacenter", datacenter,
                                "method", method,
                                "route", route,
                                "status", statusTag)
                        .publishPercentileHistogram()
                        .minimumExpectedValue(Duration.ofNanos(1_000))
                        .maximumExpectedValue(Duration.ofSeconds(5))
                        .serviceLevelObjectives(
                                Duration.ofNanos(1_000), Duration.ofNanos(2_000), Duration.ofNanos(5_000),
                                Duration.ofNanos(10_000), Duration.ofNanos(20_000), Duration.ofNanos(50_000),
                                Duration.ofNanos(100_000), Duration.ofNanos(200_000), Duration.ofNanos(500_000),
                                Duration.ofMillis(1), Duration.ofMillis(2), Duration.ofMillis(5),
                                Duration.ofMillis(10), Duration.ofMillis(25), Duration.ofMillis(50),
                                Duration.ofMillis(100), Duration.ofMillis(250), Duration.ofMillis(500),
                                Duration.ofSeconds(1), Duration.ofSeconds(2), Duration.ofSeconds(5))
                        .register(registry)
                        .record(System.nanoTime() - startedAt, TimeUnit.NANOSECONDS);
            }
        }
    }
}
