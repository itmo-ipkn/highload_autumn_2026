package io.github.dmuscipula.highloadlab.controller

import io.github.dmuscipula.highloadlab.dto.HealthResponse
import org.springframework.boot.availability.ApplicationAvailability
import org.springframework.boot.availability.ReadinessState
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.server.ResponseStatusException

@RestController
class HealthController(private val availability: ApplicationAvailability) {

    @GetMapping("/health")
    fun health(): HealthResponse {
        if (availability.readinessState != ReadinessState.ACCEPTING_TRAFFIC) {
            throw ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Application is not ready")
        }
        return HealthResponse(status = "UP")
    }
}
