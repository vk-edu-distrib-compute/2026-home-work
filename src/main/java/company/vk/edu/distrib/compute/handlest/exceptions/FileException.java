package company.vk.edu.distrib.compute.handlest.exceptions;

import java.io.Serial;

public class FileException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 1L;

    public FileException(String message) {
        super(message);
    }

    public FileException(String message, Throwable cause) {
        super(message, cause);
    }
}
