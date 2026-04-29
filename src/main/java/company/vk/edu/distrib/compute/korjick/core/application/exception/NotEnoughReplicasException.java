package company.vk.edu.distrib.compute.korjick.core.application.exception;

public class NotEnoughReplicasException extends RuntimeException {
    public NotEnoughReplicasException(String message) {
        super(message);
    }
}
