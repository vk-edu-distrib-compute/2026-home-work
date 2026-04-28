package company.vk.edu.distrib.compute.wedwincode.exceptions;

import java.io.Serial;

public class ReplicationException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 1497177566784067528L;

    public ReplicationException(String message, Throwable cause) {
        super(message, cause);
    }
}
