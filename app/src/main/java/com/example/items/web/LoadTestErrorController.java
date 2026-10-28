package com.example.items.web;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@ConditionalOnProperty(
        prefix = "loadtest.errors",
        name = "enabled",
        havingValue = "true"
)
public class LoadTestErrorController {

    @GetMapping("/__loadtest__/500")
    public ResponseEntity<Map<String, String>> serverError() {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "synthetic_load_test_error"));
    }
}
