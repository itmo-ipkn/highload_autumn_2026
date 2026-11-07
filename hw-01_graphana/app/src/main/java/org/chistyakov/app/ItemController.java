package org.chistyakov.app;

import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.ThreadLocalRandom;

@RestController
@RequestMapping("/items")
@RequiredArgsConstructor
public class ItemController {

    private final ItemService itemService;

    @Value("${FAILURE_RATE:0}")
    private double failureRate;

    @PostMapping
    public ResponseEntity<Item> createItem(@RequestBody String text) {
        Item item = itemService.createItem(text);
        maybeFail();

        return ResponseEntity.ok(item);
    }

    @PutMapping("/{id}")
    public ResponseEntity<Item> updateItem(@PathVariable String id,  @RequestBody String text) {
        try {
            Item item = itemService.updateItem(id, text);
            maybeFail();

            return ResponseEntity.ok(item);
        } catch (EntityNotFoundException e) {
            return ResponseEntity.notFound().build();
        }

    }

    @GetMapping("/{id}")
    public ResponseEntity<Item> getItem(@PathVariable String id) {
        try {
            Item item = itemService.getItemById(id);
            maybeFail();

            return ResponseEntity.ok(item);
        } catch (EntityNotFoundException e) {
            return ResponseEntity.notFound().build();
        }
    }

    private void maybeFail() {
        if (ThreadLocalRandom.current().nextDouble() < failureRate) {
            throw new IllegalStateException("injected failure");
        }
    }
}
