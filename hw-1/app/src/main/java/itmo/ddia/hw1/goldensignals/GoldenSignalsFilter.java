package itmo.ddia.hw1.goldensignals;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.HandlerMapping;

public class GoldenSignalsFilter extends OncePerRequestFilter {

    public static final String REQUEST_DURATION = "http.handler.request.duration";
    public static final String REQUESTS = "http.handler.requests";

    private static final Duration[] LATENCY_BUCKETS = {
            ms(0.25), ms(0.5), ms(1), ms(2.5), ms(5), ms(10), ms(25),
            ms(50), ms(100), ms(250), ms(500), ms(1_000), ms(2_500),
    };

    private final GoldenSignalsProperties properties;
    private final Meter.MeterProvider<Timer> requestDuration;
    private final Meter.MeterProvider<Counter> requests;

    public GoldenSignalsFilter(MeterRegistry registry, GoldenSignalsProperties properties) {
        this.properties = properties;
        this.requestDuration = Timer.builder(REQUEST_DURATION)
                .description("Time spent handling a request")
                .serviceLevelObjectives(LATENCY_BUCKETS)
                .withRegistry(registry);
        this.requests = Counter.builder(REQUESTS)
                .description("Handled requests by response status code")
                .withRegistry(registry);
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        long startNanos = System.nanoTime();
        boolean completed = false;
        try {
            chain.doFilter(request, response);
            completed = true;
        } finally {
            record(request, response, completed, System.nanoTime() - startNanos);
        }
    }

    private void record(HttpServletRequest request, HttpServletResponse response, boolean completed, long nanos) {
        if (!(request.getAttribute(HandlerMapping.BEST_MATCHING_HANDLER_ATTRIBUTE) instanceof HandlerMethod)) {
            return;
        }

        Object pattern = request.getAttribute(HandlerMapping.BEST_MATCHING_PATTERN_ATTRIBUTE);
        if (pattern == null || properties.isExcluded(pattern.toString())) {
            return;
        }

        int status = completed || response.isCommitted() ? response.getStatus() : 500;

        Tags handler = Tags.of("method", request.getMethod(), "path", pattern.toString());
        requestDuration.withTags(handler).record(nanos, TimeUnit.NANOSECONDS);
        requests.withTags(handler.and("status_code", Integer.toString(status))).increment();
    }

    private static Duration ms(double millis) {
        return Duration.ofNanos(Math.round(millis * 1_000_000));
    }
}
