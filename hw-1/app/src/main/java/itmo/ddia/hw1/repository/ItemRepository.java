package itmo.ddia.hw1.repository;

import itmo.ddia.hw1.model.Item;

import java.util.Optional;

public interface ItemRepository {

    Item create(String name, String description);

    Optional<Item> findById(long id);

    Optional<Item> update(long id, String name, String description);
}
