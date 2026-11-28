package ru.itmo.ddia.kafka.model;

public class PoisonEventException extends RuntimeException {
    public PoisonEventException(String message) {
        super(message);
    }
}
