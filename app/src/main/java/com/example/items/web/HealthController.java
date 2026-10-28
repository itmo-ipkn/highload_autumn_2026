package com.example.items.web;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HealthController {

    private final String podId;
    private final String datacenter;

    public HealthController(@Value("${POD_ID:unknown}") String podId,
                            @Value("${DATACENTER:unknown}") String datacenter) {
        this.podId = podId;
        this.datacenter = datacenter;
    }

    @GetMapping("/health")
    public HealthResponse health() {
        return new HealthResponse("UP", podId, datacenter);
    }
}
