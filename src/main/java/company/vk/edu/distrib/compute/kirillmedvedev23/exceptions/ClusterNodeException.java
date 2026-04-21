package company.vk.edu.distrib.compute.kirillmedvedev23.exceptions;

public class ClusterNodeException extends RuntimeException {
    public ClusterNodeException(String message) {
        super(message);
    }

    public ClusterNodeException(String message, Throwable cause) {
        super(message, cause);
    }
}
