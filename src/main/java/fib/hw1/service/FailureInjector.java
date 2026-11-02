package fib.hw1.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

@Component
public class FailureInjector {
    private final double failureRate;

    public FailureInjector(@Value("${app.failure-rate:0.05}") double failureRate) {
        this.failureRate = Math.max(0.0, Math.min(1.0, failureRate));
    }

    public void maybeFail() {
        if (failureRate > 0 && ThreadLocalRandom.current().nextDouble() < failureRate) {
            throw new SimulatedFailureException("A random test failure was injected");
        }
    }
}
