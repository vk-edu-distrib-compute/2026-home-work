package company.vk.edu.distrib.compute.wedwincode.task5.node;

import java.io.Serial;

public class ClusterException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 3632892236018414856L;

    public ClusterException(String message, Throwable cause) {
        super(message, cause);
    }
}
