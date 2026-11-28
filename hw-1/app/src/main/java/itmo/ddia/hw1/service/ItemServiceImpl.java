package itmo.ddia.hw1.service;

import itmo.ddia.hw1.exception.ItemNotFoundException;
import itmo.ddia.hw1.model.Item;
import itmo.ddia.hw1.repository.ItemRepository;
import org.springframework.stereotype.Service;

@Service
public class ItemServiceImpl implements ItemService {

    private final ItemRepository itemRepository;

    public ItemServiceImpl(ItemRepository itemRepository) {
        this.itemRepository = itemRepository;
    }

    @Override
    public Item create(String name, String description) {
        return itemRepository.create(name, description);
    }

    @Override
    public Item get(long id) {
        return itemRepository.findById(id)
                .orElseThrow(() -> new ItemNotFoundException(id));
    }

    @Override
    public Item update(long id, String name, String description) {
        return itemRepository.update(id, name, description)
                .orElseThrow(() -> new ItemNotFoundException(id));
    }
}
