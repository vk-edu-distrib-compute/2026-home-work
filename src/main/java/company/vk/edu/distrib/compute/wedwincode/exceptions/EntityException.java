package company.vk.edu.distrib.compute.wedwincode.exceptions;

import java.io.Serial;

public class EntityException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = -9106674164965371058L;

    public EntityException(String message, Throwable cause) {
        super(message, cause);
    }
}
