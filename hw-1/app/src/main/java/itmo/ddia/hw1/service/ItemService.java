package itmo.ddia.hw1.service;

import itmo.ddia.hw1.model.Item;

public interface ItemService {

    Item create(String name, String description);

    Item get(long id);

    Item update(long id, String name, String description);
}
