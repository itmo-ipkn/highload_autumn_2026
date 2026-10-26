package io.github.dmuscipula.highloadlab.service

import io.github.dmuscipula.highloadlab.domain.Item
import io.github.dmuscipula.highloadlab.repository.ItemRepository
import org.springframework.stereotype.Service

@Service
class ItemService(private val repository: ItemRepository) {

    fun create(name: String): Item = repository.create(name)

    fun findById(id: Long): Item? = repository.findById(id)

    fun update(id: Long, name: String): Item? = repository.update(id, name)
}
