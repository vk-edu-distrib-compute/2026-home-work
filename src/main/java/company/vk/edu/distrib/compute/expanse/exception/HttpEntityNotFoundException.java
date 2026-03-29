package company.vk.edu.distrib.compute.expanse.exception;

public class HttpEntityNotFoundException extends HttpRequestException {
    public HttpEntityNotFoundException(String message, Throwable e) {
        super(message, 404, e);
    }
}
