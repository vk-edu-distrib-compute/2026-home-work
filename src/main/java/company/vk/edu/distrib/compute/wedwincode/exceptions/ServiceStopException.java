package company.vk.edu.distrib.compute.wedwincode.exceptions;

public class ServiceStopException extends RuntimeException {
    public ServiceStopException(String message) {
        super(message);
    }

    public ServiceStopException(String message, Throwable cause) {
        super(message, cause);
    }
}
