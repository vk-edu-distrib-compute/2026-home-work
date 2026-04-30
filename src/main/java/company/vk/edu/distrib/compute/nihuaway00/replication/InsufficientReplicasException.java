package company.vk.edu.distrib.compute.nihuaway00.replication;

import java.io.Serial;

public class InsufficientReplicasException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 1L;

    public InsufficientReplicasException() {
        super();
    }

    public InsufficientReplicasException(String message) {
        super(message);
    }
}
