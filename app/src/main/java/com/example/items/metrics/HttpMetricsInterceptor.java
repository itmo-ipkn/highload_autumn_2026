package com.example.items.metrics;


import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.HandlerMapping;

import java.util.concurrent.TimeUnit;

@Component
public class HttpMetricsInterceptor implements HandlerInterceptor {

    private static final String START_NANOS =
            HttpMetricsInterceptor.class.getName() + ".startNanos";

    private final MeterRegistry registry;

    public HttpMetricsInterceptor(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public boolean preHandle(
            HttpServletRequest request,
            HttpServletResponse response,
            Object handler
    ) {
        request.setAttribute(START_NANOS, System.nanoTime());
        return true;
    }

    @Override
    public void afterCompletion(
            HttpServletRequest request,
            HttpServletResponse response,
            Object handler,
            Exception ex
    ) {
        Object startValue = request.getAttribute(START_NANOS);
        if (!(startValue instanceof Long startNanos)) {
            return;
        }

        String uri = routeTemplate(request);
        int status = response.getStatus();

        // Если исключение не было корректно отражено в HTTP-статусе,
        // учитываем его как серверную ошибку в метриках.
        if (ex != null && status < 400) {
            status = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
        }

        Tags tags = Tags.of(
                "method", request.getMethod(),
                "uri", uri,
                "status", Integer.toString(status)
        );

        Counter.builder("http.requests")
                .description("Total completed HTTP requests")
                .tags(tags)
                .register(registry)
                .increment();

        if (status >= 400) {
            Counter.builder("http.errors")
                    .description("Total HTTP requests with error status")
                    .tags(tags)
                    .register(registry)
                    .increment();
        }

        long elapsedNanos = System.nanoTime() - startNanos;

        Timer.builder("http.request.duration")
                .description("HTTP request processing duration")
                .tags(tags)
                .register(registry)
                .record(elapsedNanos, TimeUnit.NANOSECONDS);

        request.removeAttribute(START_NANOS);
    }

    private String routeTemplate(HttpServletRequest request) {
        Object pattern = request.getAttribute(
                HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE
        );

        return pattern == null ? "UNKNOWN" : pattern.toString();
    }
}
