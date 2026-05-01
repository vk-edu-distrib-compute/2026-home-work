package company.vk.edu.distrib.compute.expanse.exception;

public class EntityNotFoundException extends RequestException {
    public EntityNotFoundException(String message, Throwable e) {
        super(message, 404, e);
    }
}
