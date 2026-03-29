package company.vk.edu.distrib.compute.expanse.exception;

public class HttpMethodNotSupportedException extends HttpRequestException {
    public HttpMethodNotSupportedException(String message) {
        super(message, 405);
    }
}
