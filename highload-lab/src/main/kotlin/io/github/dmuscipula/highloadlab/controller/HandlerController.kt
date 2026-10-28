package io.github.dmuscipula.highloadlab.controller

import io.github.dmuscipula.highloadlab.dto.HandlerResponse
import io.github.dmuscipula.highloadlab.service.HandlerCatalog
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController

@RestController
class HandlerController(private val catalog: HandlerCatalog) {

    @GetMapping("/handlers")
    fun findAll(): List<HandlerResponse> = catalog.findAll()
}
