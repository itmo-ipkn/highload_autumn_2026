package io.github.dmuscipula.highloadlab.service

import io.github.dmuscipula.highloadlab.HighloadLabApplication
import io.github.dmuscipula.highloadlab.dto.HandlerResponse
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Service
import org.springframework.web.bind.annotation.RequestMethod
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping

@Service
class HandlerCatalog(
    @Qualifier("requestMappingHandlerMapping")
    private val handlerMapping: RequestMappingHandlerMapping,
) {

    private val applicationPackage = HighloadLabApplication::class.java.packageName

    fun findAll(): List<HandlerResponse> {
        val handlers = mutableSetOf<HandlerResponse>()

        for ((mapping, handler) in handlerMapping.handlerMethods) {
            val controllerPackage = handler.beanType.packageName
            if (controllerPackage != applicationPackage &&
                !controllerPackage.startsWith("$applicationPackage.")
            ) {
                continue
            }

            val methods = mapping.methodsCondition.methods.ifEmpty { RequestMethod.entries.toSet() }
            for (path in mapping.patternValues) {
                val technicalRoute =
                    path == "/handlers" ||
                        path == "/metrics" ||
                        path == "/health" ||
                        path.startsWith("/health/")
                if (technicalRoute) {
                    continue
                }

                for (method in methods) {
                    handlers += HandlerResponse(method = method.name, path = path)
                }
            }
        }

        return handlers.sortedWith(compareBy(HandlerResponse::path, HandlerResponse::method))
    }
}
