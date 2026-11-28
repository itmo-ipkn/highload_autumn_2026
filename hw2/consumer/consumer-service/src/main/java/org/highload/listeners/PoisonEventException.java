package org.highload.listeners;

public class PoisonEventException extends RuntimeException {
    public PoisonEventException(String message) {
        super(message);
    }
}
