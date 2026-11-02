package fib.hw1.service;

public class SimulatedFailureException extends RuntimeException {
    public SimulatedFailureException(String message) {
        super(message);
    }
}
