import com.fasterxml.jackson.annotation.JsonValue;

public enum EventType {
    PAGE_VIEW("page_view"),
    ADD_TO_CART("add_to_cart"),
    PURCHASE("purchase"),;

    private final String type;

    EventType(String type) {
        this.type = type;
    }

    @JsonValue
    public String getType() {
        return type;
    }
}
