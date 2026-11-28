package itmo.ddia.hw1.goldensignals;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.config.MeterFilter;
import jakarta.servlet.DispatcherType;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(GoldenSignalsProperties.class)
public class GoldenSignalsConfiguration {

    @Bean
    MeterFilter goldenSignalsCommonTags(GoldenSignalsProperties properties) {
        return MeterFilter.commonTags(Tags.of(
                "pod_id", properties.podId(),
                "datacenter", properties.datacenter()));
    }

    @Bean
    MeterFilter denyBuiltInHttpServerMetrics() {
        return MeterFilter.denyNameStartsWith("http.server.requests");
    }

    @Bean
    FilterRegistrationBean<GoldenSignalsFilter> goldenSignalsFilter(
            MeterRegistry registry, GoldenSignalsProperties properties) {
        var registration = new FilterRegistrationBean<>(new GoldenSignalsFilter(registry, properties));
        registration.setOrder(Ordered.HIGHEST_PRECEDENCE + 10);
        registration.setDispatcherTypes(DispatcherType.REQUEST);
        return registration;
    }
}
