package io.github.dmuscipula.highloadlab.controller

import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc
import org.springframework.test.json.JsonCompareMode
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.get
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.bind.annotation.ResponseBody
import org.springframework.web.servlet.mvc.method.RequestMappingInfo
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping

@SpringBootTest
@AutoConfigureMockMvc
class HandlerControllerTest {

    @Autowired
    private lateinit var mockMvc: MockMvc

    @Autowired
    @Qualifier("requestMappingHandlerMapping")
    private lateinit var handlerMapping: RequestMappingHandlerMapping

    @Test
    fun `catalog discovers a newly registered business route`() {
        mockMvc.get("/handlers").andExpect {
            status { isOk() }
            jsonPath("$.length()") { value(3) }
        }

        val mapping = RequestMappingInfo.paths("/catalog-probe")
            .methods(RequestMethod.GET)
            .options(handlerMapping.builderConfiguration)
            .build()
        val handler = AdditionalHandler()
        handlerMapping.registerMapping(mapping, handler, AdditionalHandler::class.java.getMethod("handle"))

        try {
            mockMvc.get("/handlers").andExpect {
                status { isOk() }
                content {
                    json(
                        """[
                            {"method":"GET","path":"/catalog-probe"},
                            {"method":"POST","path":"/items"},
                            {"method":"GET","path":"/items/{id}"},
                            {"method":"PUT","path":"/items/{id}"}
                        ]""",
                        JsonCompareMode.STRICT,
                    )
                }
            }
        } finally {
            handlerMapping.unregisterMapping(mapping)
        }
    }

    class AdditionalHandler {
        @ResponseBody
        fun handle(): String = "probe"
    }
}
