package itmo.ddia.hw1.repository;

import itmo.ddia.hw1.model.Item;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

@Repository
public class ItemRepositoryInMemory implements ItemRepository {

    private final ConcurrentMap<Long, Item> items = new ConcurrentHashMap<>();
    private final AtomicLong sequence = new AtomicLong();

    @Override
    public Item create(String name, String description) {
        long id = sequence.incrementAndGet();
        Item item = new Item(id, name, description);
        items.put(id, item);
        return item;
    }

    @Override
    public Optional<Item> findById(long id) {
        return Optional.ofNullable(items.get(id));
    }

    @Override
    public Optional<Item> update(long id, String name, String description) {
        return Optional.ofNullable(
                items.computeIfPresent(id, (key, old) -> new Item(key, name, description)));
    }
}
