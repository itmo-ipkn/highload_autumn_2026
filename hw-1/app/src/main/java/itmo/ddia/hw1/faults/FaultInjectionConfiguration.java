package itmo.ddia.hw1.faults;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(FaultInjectionProperties.class)
public class FaultInjectionConfiguration implements WebMvcConfigurer {

    private final FaultInjectionProperties properties;

    public FaultInjectionConfiguration(FaultInjectionProperties properties) {
        this.properties = properties;
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(new FaultInjectionInterceptor(properties.probability()))
                .addPathPatterns("/items", "/items/**");
    }
}
