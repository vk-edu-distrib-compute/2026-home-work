package company.vk.edu.distrib.compute.expanse.exception;

public class HttpBadRequestException extends HttpRequestException {
    public HttpBadRequestException(String message) {
        super(message, 400);
    }
}
