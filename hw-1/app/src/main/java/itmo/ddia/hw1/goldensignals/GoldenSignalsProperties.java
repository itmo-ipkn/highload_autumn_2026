package itmo.ddia.hw1.goldensignals;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties("golden-signals")
public record GoldenSignalsProperties(
        String podId,
        String datacenter,
        @DefaultValue({"/health", "/metrics", "/handlers", "/error"}) List<String> excludedPaths) {

    public boolean isExcluded(String pathPattern) {
        return excludedPaths.stream().anyMatch(excluded ->
                pathPattern.equals(excluded) || pathPattern.startsWith(excluded + "/"));
    }
}
