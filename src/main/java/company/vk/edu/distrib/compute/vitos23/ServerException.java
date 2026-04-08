package company.vk.edu.distrib.compute.vitos23;

import java.io.Serial;

public class ServerException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 3L;

    public ServerException(Throwable cause) {
        super(cause);
    }
}
