package company.vk.edu.distrib.compute.handlest.exceptions;

import java.io.Serial;

public class ServerStopException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 1L;

    public ServerStopException(String message) {
        super(message);
    }

    public ServerStopException(String message, Throwable cause) {
        super(message, cause);
    }

}
