package itmo.ddia.hw1.controller;

import itmo.ddia.hw1.dto.ItemRequest;
import itmo.ddia.hw1.dto.ItemResponse;
import itmo.ddia.hw1.service.ItemService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;

@RestController
@RequestMapping("/items")
public class ItemController {

    private final ItemService itemService;

    public ItemController(ItemService itemService) {
        this.itemService = itemService;
    }

    @PostMapping
    public ResponseEntity<ItemResponse> create(@Valid @RequestBody ItemRequest request) {
        ItemResponse item = ItemResponse.from(itemService.create(request.name(), request.description()));
        return ResponseEntity.created(URI.create("/items/" + item.id())).body(item);
    }

    @GetMapping("/{id}")
    public ItemResponse get(@PathVariable long id) {
        return ItemResponse.from(itemService.get(id));
    }

    @PutMapping("/{id}")
    public ItemResponse update(@PathVariable long id, @Valid @RequestBody ItemRequest request) {
        return ItemResponse.from(itemService.update(id, request.name(), request.description()));
    }
}
