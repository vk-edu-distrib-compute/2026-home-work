package company.vk.edu.distrib.compute.handlest.exceptions;

import java.io.Serial;

public class HttpClientException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 1L;

    public HttpClientException(String message) {
        super(message);
    }

    public HttpClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
