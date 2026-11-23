package org.highload.repositories;

import org.highload.entities.Item;
import org.highload.exceptions.InternalServerException;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Repository
public class ItemRepository {

    private static final int MAX_WAIT_MIN = 1;
    private final List<Item> items = new ArrayList<>();
    private Long lastId = 0L;
    private final Lock lock = new ReentrantLock();

    public Long save(Item item) {
        acquireLock();
        try {
            Item newItem = new Item(++lastId, item.getName(), item.getDescription());
            items.add(newItem);
            return lastId;
        } finally {
            lock.unlock();
        }
    }

    public Optional<Item> findById(Long id) {
        acquireLock();
        try {
            return items.stream()
                    .filter(item -> item.getId().equals(id))
                    .findFirst()
                    .map(this::copy);
        } finally {
            lock.unlock();
        }
    }

    public Optional<Item> updateById(Long id, Item item) {
        acquireLock();
        try {
            for (int i = 0; i < items.size(); i++) {
                Item curr = items.get(i);
                if (!curr.getId().equals(id)) {
                    continue;
                }

                Item updated = new Item(id, item.getName(), item.getDescription());
                items.set(i, updated);
                return Optional.of(copy(updated));
            }
            return Optional.empty();
        } finally {
            lock.unlock();
        }
    }

    private Item copy(Item item) {
        return new Item(
                item.getId(),
                item.getName(),
                item.getDescription()
        );
    }

    private void acquireLock() {
        try {
            boolean acquired = lock.tryLock(MAX_WAIT_MIN, TimeUnit.MINUTES);

            if (!acquired) {
                throw new InternalServerException("Timeout take lock");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InternalServerException("Interrupted while acquiring repository lock");
        }
    }

}
