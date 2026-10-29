package io.github.dmuscipula.highloadlab.controller

import io.github.dmuscipula.highloadlab.dto.CreateItemRequest
import io.github.dmuscipula.highloadlab.dto.ItemResponse
import io.github.dmuscipula.highloadlab.dto.UpdateItemRequest
import io.github.dmuscipula.highloadlab.service.ItemService
import io.github.dmuscipula.highloadlab.service.RequestSimulator
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.PutMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.server.ResponseStatusException

@RestController
@RequestMapping("/items")
class ItemController(
    private val service: ItemService,
    private val requestSimulator: RequestSimulator,
) {

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    fun create(@RequestBody request: CreateItemRequest): ItemResponse {
        requestSimulator.simulateWork()
        val item = service.create(request.name)
        return ItemResponse(id = item.id, name = item.name)
    }

    @GetMapping("/{id}")
    fun findById(@PathVariable("id") id: Long): ItemResponse {
        requestSimulator.simulateWork()
        val item = service.findById(id)
            ?: throw ResponseStatusException(HttpStatus.NOT_FOUND, "Item not found")
        return ItemResponse(id = item.id, name = item.name)
    }

    @PutMapping("/{id}")
    fun update(
        @PathVariable("id") id: Long,
        @RequestBody request: UpdateItemRequest,
    ): ItemResponse {
        requestSimulator.simulateWork()
        val item = service.update(id, request.name)
            ?: throw ResponseStatusException(HttpStatus.NOT_FOUND, "Item not found")
        return ItemResponse(id = item.id, name = item.name)
    }
}
