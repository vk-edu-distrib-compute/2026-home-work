package company.vk.edu.distrib.compute.wedwincode.exceptions;

import java.io.Serial;

public class ServiceStartException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = -7810503583243373994L;

    public ServiceStartException(String message) {
        super(message);
    }
}
