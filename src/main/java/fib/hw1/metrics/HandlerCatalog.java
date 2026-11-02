package fib.hw1.metrics;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.mvc.method.RequestMappingInfo;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import java.util.Comparator;
import java.util.List;
import java.util.Set;

@Component
public class HandlerCatalog {
    private static final Set<String> EXCLUDED_ROUTES = Set.of("/health", "/metrics", "/handlers");

    private final ObjectProvider<RequestMappingHandlerMapping> mappings;

    public HandlerCatalog(ObjectProvider<RequestMappingHandlerMapping> mappings) {
        this.mappings = mappings;
    }

    public List<HandlerDescriptor> list() {
        return mappings.getObject().getHandlerMethods().keySet().stream()
                .flatMap(info -> descriptorsFor(info).stream())
                .filter(descriptor -> !EXCLUDED_ROUTES.contains(descriptor.route()))
                .distinct()
                .sorted(Comparator.comparing(HandlerDescriptor::route)
                        .thenComparing(HandlerDescriptor::method))
                .toList();
    }

    private List<HandlerDescriptor> descriptorsFor(RequestMappingInfo info) {
        Set<String> paths = info.getPatternValues();
        Set<RequestMethod> methods = info.getMethodsCondition().getMethods();
        return paths.stream()
                .filter(path -> !path.isBlank())
                .flatMap(path -> methods.stream().map(method -> new HandlerDescriptor(method.name(), path)))
                .toList();
    }
}
