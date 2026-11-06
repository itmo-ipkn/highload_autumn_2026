package org.highload.controllers;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.highload.dto.ItemDto;
import org.highload.dto.ItemRequestDto;
import org.highload.services.ItemService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
@RequestMapping("/items")
public class ItemController {

    private final ItemService itemService;

    @PostMapping
    public ResponseEntity<Long> create(@RequestBody @Valid ItemRequestDto itemRequestDto) {
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(itemService.create(itemRequestDto));
    }

    @GetMapping("/{id}")
    public ResponseEntity<ItemDto> getById(@PathVariable Long id) {
        return ResponseEntity.ok(itemService.getById(id));
    }

    @PutMapping("/{id}")
    public ResponseEntity<ItemDto> update(@PathVariable Long id, @RequestBody @Valid ItemRequestDto updateDto) {
        return ResponseEntity.ok(itemService.updateById(id, updateDto));
    }

}
