package itmo.ddia.hw1.dto;

import itmo.ddia.hw1.model.Item;

public record ItemResponse(long id, String name, String description) {

    public static ItemResponse from(Item item) {
        return new ItemResponse(item.id(), item.name(), item.description());
    }
}
