package itmo.ddia.hw1.exception;

public class ItemNotFoundException extends RuntimeException {

    public ItemNotFoundException(long id) {
        super("Item " + id + " not found");
    }
}
