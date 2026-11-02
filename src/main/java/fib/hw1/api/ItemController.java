package fib.hw1.api;

import fib.hw1.model.Item;
import fib.hw1.model.ItemRequest;
import fib.hw1.service.FailureInjector;
import fib.hw1.service.ItemStore;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/items")
public class ItemController {
    private final ItemStore store;
    private final FailureInjector failureInjector;

    public ItemController(ItemStore store, FailureInjector failureInjector) {
        this.store = store;
        this.failureInjector = failureInjector;
    }

    @PostMapping
    public ResponseEntity<Item> create(@RequestBody ItemRequest request) {
        failureInjector.maybeFail();
        return ResponseEntity.status(HttpStatus.CREATED).body(store.create(request));
    }

    @GetMapping("/{id}")
    public Item get(@PathVariable long id) {
        failureInjector.maybeFail();
        Item item = store.get(id);
        if (item == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Item " + id + " was not found");
        }
        return item;
    }

    @PutMapping("/{id}")
    public Item update(@PathVariable long id, @RequestBody ItemRequest request) {
        failureInjector.maybeFail();
        Item updated = store.update(id, request);
        if (updated == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Item " + id + " was not found");
        }
        return updated;
    }
}
