package company.vk.edu.distrib.compute.expanse.exception;

public class BadRequestException extends RequestException {
    public BadRequestException(String message) {
        super(message, 400);
    }
}
