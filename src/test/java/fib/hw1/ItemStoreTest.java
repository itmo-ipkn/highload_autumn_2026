package fib.hw1;

import fib.hw1.model.Item;
import fib.hw1.model.ItemRequest;
import fib.hw1.service.ItemStore;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ItemStoreTest {
    @Test
    void createGetAndUpdateItem() {
        ItemStore store = new ItemStore();
        Item created = store.create(new ItemRequest("keyboard", "mechanical"));

        assertEquals("keyboard", store.get(created.id()).name());
        Item updated = store.update(created.id(), new ItemRequest("mouse", "wireless"));
        assertEquals("mouse", updated.name());
        assertEquals("wireless", updated.description());
    }

    @Test
    void updateMissingItemReturnsNull() {
        assertNull(new ItemStore().update(999L, new ItemRequest("x", "y")));
    }
}
