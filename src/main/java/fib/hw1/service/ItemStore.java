package fib.hw1.service;

import fib.hw1.model.Item;
import fib.hw1.model.ItemRequest;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class ItemStore {
    private final ConcurrentHashMap<Long, Item> items = new ConcurrentHashMap<>();
    private final AtomicLong sequence = new AtomicLong(0);

    public Item create(ItemRequest request) {
        long id = sequence.incrementAndGet();
        Item item = new Item(id, request.name(), request.description());
        items.put(id, item);
        return item;
    }

    public Item get(long id) {
        return items.get(id);
    }

    public Item update(long id, ItemRequest request) {
        if (!items.containsKey(id)) {
            return null;
        }
        Item updated = new Item(id, request.name(), request.description());
        return items.computeIfPresent(id, (key, oldValue) -> updated);
    }
}
