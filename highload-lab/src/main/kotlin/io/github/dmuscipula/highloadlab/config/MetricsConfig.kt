package io.github.dmuscipula.highloadlab.config

import io.micrometer.core.instrument.config.MeterFilter
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class MetricsConfig {

    @Bean
    fun removeUnusedHttpTags(): MeterFilter = MeterFilter.forMeters(
        { id -> id.name == "http.server.requests" },
        MeterFilter.ignoreTags("outcome", "error", "exception"),
    )

    @Bean
    fun excludeServiceHttpMetrics(): MeterFilter = MeterFilter.deny { id ->
        val uri = id.getTag("uri").orEmpty()
        val technicalRoute =
            uri == "/handlers" ||
                uri == "/metrics" ||
                uri == "/health" ||
                uri.startsWith("/health/")

        id.name == "http.server.requests" && technicalRoute
    }
}
