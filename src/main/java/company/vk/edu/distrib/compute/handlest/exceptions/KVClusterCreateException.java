package company.vk.edu.distrib.compute.handlest.exceptions;

import java.io.Serial;

public class KVClusterCreateException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 1L;

    public KVClusterCreateException(String message) {
        super(message);
    }

    public KVClusterCreateException(String message, Throwable cause) {
        super(message, cause);
    }

}
