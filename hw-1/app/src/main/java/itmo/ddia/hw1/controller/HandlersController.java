package itmo.ddia.hw1.controller;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import itmo.ddia.hw1.goldensignals.GoldenSignalsProperties;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.RequestMappingInfo;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

@RestController
public class HandlersController {

    private final RequestMappingHandlerMapping handlerMapping;
    private final GoldenSignalsProperties properties;

    public HandlersController(
            @Qualifier("requestMappingHandlerMapping") RequestMappingHandlerMapping handlerMapping,
            GoldenSignalsProperties properties) {
        this.handlerMapping = handlerMapping;
        this.properties = properties;
    }

    @GetMapping("/handlers")
    public List<Handler> handlers() {
        return handlerMapping.getHandlerMethods().keySet().stream()
                .flatMap(HandlersController::toHandlers)
                .filter(handler -> !properties.isExcluded(handler.path()))
                .distinct()
                .sorted(Comparator.comparing(Handler::path).thenComparing(Handler::method))
                .toList();
    }

    private static Stream<Handler> toHandlers(RequestMappingInfo info) {
        return info.getPatternValues().stream().flatMap(path ->
                info.getMethodsCondition().getMethods().stream()
                        .map(method -> new Handler(method.name(), path)));
    }

    public record Handler(String method, String path) {
    }
}
