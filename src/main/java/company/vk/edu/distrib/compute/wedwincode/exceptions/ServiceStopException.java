package company.vk.edu.distrib.compute.wedwincode.exceptions;

import java.io.Serial;

public class ServiceStopException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = -3679390175207482400L;

    public ServiceStopException(String message) {
        super(message);
    }

    public ServiceStopException(String message, Throwable cause) {
        super(message, cause);
    }
}
