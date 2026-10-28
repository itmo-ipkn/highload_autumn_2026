package com.example.items.web;

import com.example.items.model.Item;

import java.time.Instant;

public record ItemResponse(String id, String value, Instant createdAt, Instant updatedAt) {

    public static ItemResponse from(Item item) {
        return new ItemResponse(item.id(), item.value(), item.createdAt(), item.updatedAt());
    }
}
