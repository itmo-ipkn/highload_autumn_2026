package org.chistyakov.app;

import jakarta.persistence.EntityNotFoundException;
import lombok.AllArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/items")
@AllArgsConstructor
public class ItemController {

    ItemService itemService;

    @PostMapping
    public ResponseEntity<Item> createItem(@RequestBody String text) {
        Item item = itemService.createItem(text);

        return ResponseEntity.ok(item);
    }

    @PutMapping("/{id}")
    public ResponseEntity<Item> updateItem(@PathVariable String id,  @RequestBody String text) {
        try {
            Item item = itemService.updateItem(id, text);
            return ResponseEntity.ok(item);
        } catch (EntityNotFoundException e) {
            return ResponseEntity.notFound().build();
        }

    }

    @GetMapping("/{id}")
    public ResponseEntity<Item> getItem(@PathVariable String id) {
        try {
            Item item = itemService.getItemById(id);
            return ResponseEntity.ok(item);
        } catch (EntityNotFoundException e) {
            return ResponseEntity.notFound().build();
        }
    }
}
