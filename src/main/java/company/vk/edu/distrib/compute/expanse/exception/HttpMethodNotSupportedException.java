package company.vk.edu.distrib.compute.expanse.exception;

public class HttpMethodNotSupportedException extends RequestException {
    public HttpMethodNotSupportedException(String message) {
        super(message, 405);
    }
}
