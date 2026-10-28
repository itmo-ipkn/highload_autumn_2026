package itmo.ddia.hw1.faults;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("faults")
public record FaultInjectionProperties(double probability) {

    public FaultInjectionProperties {
        if (probability < 0 || probability > 1) {
            throw new IllegalArgumentException("faults.probability must be within [0, 1], got " + probability);
        }
    }
}
