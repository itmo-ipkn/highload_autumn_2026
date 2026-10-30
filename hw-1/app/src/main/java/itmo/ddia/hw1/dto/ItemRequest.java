package itmo.ddia.hw1.dto;

import jakarta.validation.constraints.NotBlank;

public record ItemRequest(@NotBlank String name, String description) {
}
