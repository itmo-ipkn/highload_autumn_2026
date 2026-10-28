package com.example.items.model;

import java.time.Instant;

public record Item(String id, String value, Instant createdAt, Instant updatedAt) {

    public Item withValue(String newValue) {
        return new Item(id, newValue, createdAt, Instant.now());
    }
}
