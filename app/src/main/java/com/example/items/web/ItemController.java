package com.example.items.web;

import com.example.items.model.Item;
import com.example.items.storage.InMemoryItemStore;
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

import java.net.URI;

@RestController
@RequestMapping("/items")
public class ItemController {

    private final InMemoryItemStore store;

    public ItemController(InMemoryItemStore store) {
        this.store = store;
    }

    @PostMapping
    public ResponseEntity<ItemResponse> create(@RequestBody CreateItemRequest request) {
        requireValue(request != null ? request.value() : null);
        Item item = store.create(request.value());
        return ResponseEntity
                .created(URI.create("/items/" + item.id()))
                .body(ItemResponse.from(item));
    }

    @GetMapping("/{id}")
    public ItemResponse get(@PathVariable String id) {
        return store.findById(id)
                .map(ItemResponse::from)
                .orElseThrow(() -> notFound(id));
    }

    @PutMapping("/{id}")
    public ItemResponse update(@PathVariable String id, @RequestBody UpdateItemRequest request) {
        requireValue(request != null ? request.value() : null);
        return store.update(id, request.value())
                .map(ItemResponse::from)
                .orElseThrow(() -> notFound(id));
    }

    private static void requireValue(String value) {
        if (value == null || value.isBlank()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Field 'value' is required");
        }
    }

    private static ResponseStatusException notFound(String id) {
        return new ResponseStatusException(HttpStatus.NOT_FOUND, "Item %s not found".formatted(id));
    }
}
