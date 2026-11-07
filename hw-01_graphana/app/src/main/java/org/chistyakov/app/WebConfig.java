package org.chistyakov.app;

import io.micrometer.core.instrument.config.MeterFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WebConfig {

    @Bean
    public MeterFilter excludeServiceEndpoints() {
        return MeterFilter.deny(id -> {
            String uri = id.getTag("uri");
            return uri != null && (uri.equals("/health") || uri.equals("/metrics") || uri.equals("/handlers"));
        });
    }

}
