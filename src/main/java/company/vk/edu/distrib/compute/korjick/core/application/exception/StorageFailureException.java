package company.vk.edu.distrib.compute.korjick.core.application.exception;

public class StorageFailureException extends RuntimeException {
    public StorageFailureException(String message) {
        super(message);
    }

    public StorageFailureException(String message, Throwable cause) {
        super(message, cause);
    }
}
