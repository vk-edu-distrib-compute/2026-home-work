package company.vk.edu.distrib.compute.handlest.exceptions;

import java.io.Serial;

public class PortRangeException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 1L;

    public PortRangeException(String message) {
        super(message);
    }
}
