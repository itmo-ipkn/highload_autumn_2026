package org.lib.metrics.endpoints;

import lombok.RequiredArgsConstructor;
import org.lib.metrics.dto.HandlerInfoDto;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import java.util.List;

@RequiredArgsConstructor
@Endpoint(id = "handlers")
public class HandlersEndpoint {

    private final RequestMappingHandlerMapping requestMappingHandlerMapping;

    @ReadOperation
    public List<HandlerInfoDto> handlers() {
        return requestMappingHandlerMapping.getHandlerMethods().keySet()
                .stream()
                .filter(info -> info.getPathPatternsCondition() != null)
                .flatMap(info -> {
                    var paths = info.getPathPatternsCondition().getPatternValues();
                    var methods = info.getMethodsCondition().getMethods();

                    return paths.stream()
                            .flatMap(path -> methods.stream()
                                    .map(method -> new HandlerInfoDto(
                                            method.name(),
                                            path
                                    ))
                            );
                })
                .toList();
    }

}
