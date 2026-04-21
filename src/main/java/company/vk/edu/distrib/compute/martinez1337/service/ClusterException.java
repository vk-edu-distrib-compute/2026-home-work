package company.vk.edu.distrib.compute.martinez1337.service;

import java.io.Serial;

public class ClusterException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 1L;

    public ClusterException(String message, Throwable cause) {
        super(message, cause);
    }
}
