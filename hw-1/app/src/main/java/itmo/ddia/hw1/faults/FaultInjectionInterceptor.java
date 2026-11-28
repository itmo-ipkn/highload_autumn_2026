package itmo.ddia.hw1.faults;

import java.util.concurrent.ThreadLocalRandom;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.HandlerInterceptor;

public class FaultInjectionInterceptor implements HandlerInterceptor {

    private final double probability;

    public FaultInjectionInterceptor(double probability) {
        this.probability = probability;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        if (probability > 0 && ThreadLocalRandom.current().nextDouble() < probability) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Injected fault");
        }
        return true;
    }
}
