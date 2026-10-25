package org.lib.metrics.configurations;

import org.lib.metrics.endpoints.HandlersEndpoint;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

@AutoConfiguration
@ConditionalOnClass({Endpoint.class, RequestMappingHandlerMapping.class})
public class HandlersConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public HandlersEndpoint handlersEndpoint(
            RequestMappingHandlerMapping requestMappingHandlerMapping) {
        return new HandlersEndpoint(requestMappingHandlerMapping);
    }

}
