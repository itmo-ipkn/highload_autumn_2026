package io.github.dmuscipula.highloadlab.repository

import io.github.dmuscipula.highloadlab.domain.Item
import org.springframework.stereotype.Repository
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

@Repository
class ItemRepository {

    private val items = ConcurrentHashMap<Long, Item>()
    private val nextId = AtomicLong()

    fun create(name: String): Item {
        val item = Item(id = nextId.incrementAndGet(), name = name)
        items[item.id] = item
        return item
    }

    fun findById(id: Long): Item? = items[id]

    fun update(id: Long, name: String): Item? =
        items.computeIfPresent(id) { _, item -> item.copy(name = name) }
}
