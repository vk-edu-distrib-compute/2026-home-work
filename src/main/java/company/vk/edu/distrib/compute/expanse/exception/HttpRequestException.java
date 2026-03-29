package company.vk.edu.distrib.compute.expanse.exception;

public class HttpRequestException extends RuntimeException {
    protected final int code;

    public HttpRequestException(String message, int code) {
        super(message);
        this.code = code;
    }

    public HttpRequestException(String message, int code, Throwable e) {
        super(message, e);
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
