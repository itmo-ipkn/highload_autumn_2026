package ru.itmo.ddia.kafka.model;

import com.fasterxml.jackson.annotation.JsonValue;

public enum EventType {
    PAGE_VIEW("page_view"),
    ADD_TO_CART("add_to_cart"),
    PURCHASE("purchase");

    private final String value;

    EventType(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    public static EventType fromValue(String value) {
        for (EventType type : values()) {
            if (type.value.equalsIgnoreCase(value) || type.name().equalsIgnoreCase(value)) {
                return type;
            }
        }
        return PAGE_VIEW;
    }
}
