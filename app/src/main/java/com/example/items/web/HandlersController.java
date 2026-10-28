package com.example.items.web;


import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.mvc.method.RequestMappingInfo;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import java.util.List;
import java.util.Map;
import java.util.TreeSet;

@RestController
public class HandlersController {

    private final RequestMappingHandlerMapping mappings;

    public HandlersController(@Qualifier("requestMappingHandlerMapping") RequestMappingHandlerMapping mappings) {
        this.mappings = mappings;
    }

    public record HandlerDescriptor(String method, String path) {}

    @GetMapping("/handlers")
    public List<HandlerDescriptor> handlers() {
        var result = new TreeSet<>(
                java.util.Comparator
                        .comparing(HandlerDescriptor::path)
                        .thenComparing(HandlerDescriptor::method)
        );

        for (Map.Entry<RequestMappingInfo, HandlerMethod> entry
                : mappings.getHandlerMethods().entrySet()) {

            HandlerMethod handler = entry.getValue();

            // Не включаем внутренние обработчики Spring Boot.
            if (!handler.getBeanType().getPackageName()
                    .startsWith("com.example.items")) {
                continue;
            }

            var patterns = entry.getKey().getPatternValues();
            var methods = entry.getKey().getMethodsCondition().getMethods();

            for (String path : patterns) {
                if (methods.isEmpty()) {
                    result.add(new HandlerDescriptor("*", path));
                } else {
                    methods.forEach(method ->
                            result.add(new HandlerDescriptor(
                                    method.name(), path)));
                }
            }
        }

        return List.copyOf(result);
    }
}
