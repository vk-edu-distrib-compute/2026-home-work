package company.vk.edu.distrib.compute.kirillmedvedev23.exceptions;

public class ClusterNodeException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public ClusterNodeException(String message) {
        super(message);
    }

    public ClusterNodeException(String message, Throwable cause) {
        super(message, cause);
    }
}
