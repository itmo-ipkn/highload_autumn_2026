package com.example.items.storage;

import com.example.items.model.Item;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class InMemoryItemStore {

    private final Map<String, Item> items = new ConcurrentHashMap<>();

    public Item create(String value) {
        String id = UUID.randomUUID().toString();
        Instant now = Instant.now();
        Item item = new Item(id, value, now, now);
        items.put(id, item);
        return item;
    }

    public Optional<Item> findById(String id) {
        return Optional.ofNullable(items.get(id));
    }

    public Optional<Item> update(String id, String value) {
        Item updated = items.compute(id, (key, existing) ->
                existing == null ? null : existing.withValue(value));
        return Optional.ofNullable(updated);
    }
}
