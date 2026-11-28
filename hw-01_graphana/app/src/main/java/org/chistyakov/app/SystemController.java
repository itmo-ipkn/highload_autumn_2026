package org.chistyakov.app;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import java.util.*;

@RestController
public class SystemController {

    private static final Set<String> HIDDEN = Set.of("/health", "/handlers", "/error", "/metrics");

    private final RequestMappingHandlerMapping mapping;

    public SystemController(@Qualifier("requestMappingHandlerMapping") RequestMappingHandlerMapping mapping) {
        this.mapping = mapping;
    }

    @GetMapping("/health")
    public ResponseEntity<Void> heath() {
        return ResponseEntity.ok().build();
    }

    @GetMapping("/handlers")
    public List<Map<String, String>>  handlers() {
        List<Map<String, String>> list = new ArrayList<>();
        mapping.getHandlerMethods().keySet().forEach(info -> {
            if (info.getPathPatternsCondition() == null) return;
            for (String path : info.getPathPatternsCondition().getPatternValues()) {
                if (HIDDEN.contains(path)) continue;
                for (RequestMethod method : info.getMethodsCondition().getMethods()) {
                    list.add(Map.of("method", method.name(), "path", path));
                }
            }
        });

        list.sort(Comparator.comparing((Map<String, String> h) -> h.get("path"))
                            .thenComparing(h -> h.get("method")));

        return list;
    }
}
