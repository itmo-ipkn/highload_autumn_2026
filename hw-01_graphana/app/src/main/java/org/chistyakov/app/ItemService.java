package org.chistyakov.app;

import jakarta.persistence.EntityNotFoundException;
import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
public class ItemService {

    ItemRepository itemRepository;

    public Item getItemById(String id) {
        return itemRepository.getItemById(id).orElseThrow(() -> new EntityNotFoundException(id));
    }

    public Item createItem(String text) {
        Item item = new Item();
        item.setText(text);

        return itemRepository.save(item);
    }

    public Item updateItem(String id, String text) {
        Item updatedItem = itemRepository.getItemById(id).orElseThrow(EntityNotFoundException::new);
        updatedItem.setText(text);

        return itemRepository.save(updatedItem);
    }
}
