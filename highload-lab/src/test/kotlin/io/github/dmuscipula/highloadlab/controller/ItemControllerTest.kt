package io.github.dmuscipula.highloadlab.controller

import io.github.dmuscipula.highloadlab.repository.ItemRepository
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.get
import org.springframework.test.web.servlet.post
import org.springframework.test.web.servlet.put
import kotlin.test.assertEquals

@SpringBootTest(
    properties = [
        "simulation.error-probability=0",
        "simulation.min-delay-ms=0",
        "simulation.max-delay-ms=0",
    ],
)
@AutoConfigureMockMvc
class ItemControllerTest {

    @Autowired
    private lateinit var mockMvc: MockMvc

    @Autowired
    private lateinit var repository: ItemRepository

    @Test
    fun `POST items returns the created item`() {
        mockMvc.post("/items") {
            contentType = MediaType.APPLICATION_JSON
            content = """{"name":"Книга"}"""
        }.andExpect {
            status { isCreated() }
            content { contentTypeCompatibleWith(MediaType.APPLICATION_JSON) }
            jsonPath("$.id") { isNumber() }
            jsonPath("$.name") { value("Книга") }
        }
    }

    @Test
    fun `POST items without a name returns bad request`() {
        mockMvc.post("/items") {
            contentType = MediaType.APPLICATION_JSON
            content = "{}"
        }.andExpect {
            status { isBadRequest() }
        }
    }

    @Test
    fun `GET item returns the stored item`() {
        val item = repository.create("Книга")

        mockMvc.get("/items/${item.id}").andExpect {
            status { isOk() }
            content { contentTypeCompatibleWith(MediaType.APPLICATION_JSON) }
            jsonPath("$.id") { value(item.id) }
            jsonPath("$.name") { value("Книга") }
        }
    }

    @Test
    fun `GET missing item returns not found`() {
        mockMvc.get("/items/${Long.MAX_VALUE}").andExpect {
            status { isNotFound() }
        }
    }

    @Test
    fun `PUT item changes the name and keeps the id`() {
        val item = repository.create("Книга")

        mockMvc.put("/items/${item.id}") {
            contentType = MediaType.APPLICATION_JSON
            content = """{"name":"Тетрадь"}"""
        }.andExpect {
            status { isOk() }
            jsonPath("$.id") { value(item.id) }
            jsonPath("$.name") { value("Тетрадь") }
        }

        assertEquals("Тетрадь", repository.findById(item.id)?.name)
    }

    @Test
    fun `PUT missing item returns not found without creating it`() {
        mockMvc.put("/items/${Long.MAX_VALUE}") {
            contentType = MediaType.APPLICATION_JSON
            content = """{"name":"Тетрадь"}"""
        }.andExpect {
            status { isNotFound() }
        }

        assertEquals(null, repository.findById(Long.MAX_VALUE))
    }
}
