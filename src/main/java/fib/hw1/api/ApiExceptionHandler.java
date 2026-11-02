package fib.hw1.api;

import fib.hw1.service.SimulatedFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

@RestControllerAdvice
public class ApiExceptionHandler {
    @ExceptionHandler(SimulatedFailureException.class)
    public ResponseEntity<Map<String, String>> handleInjectedFailure(SimulatedFailureException exception) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "simulated_failure", "message", exception.getMessage()));
    }
}
