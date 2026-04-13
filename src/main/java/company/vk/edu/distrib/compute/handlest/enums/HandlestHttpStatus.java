package company.vk.edu.distrib.compute.handlest.enums;

public enum HandlestHttpStatus {
    // 2xx Success
    OK(200, "OK"),
    CREATED(201, "Created"),
    ACCEPTED(202, "Accepted"),

    // 4xx Client errors
    BAD_REQUEST(400, "Bad Request"),
    UNAUTHORIZED(401, "Unauthorized"),
    FORBIDDEN(403, "Forbidden"),
    NOT_FOUND(404, "Not Found"),
    METHOD_NOT_ALLOWED(405, "Method Not Allowed"),

    // 5xx Server errors
    INTERNAL_ERROR(500, "Internal Server Error"),
    SERVICE_UNAVAILABLE(503, "Service Unavailable");

    private final int statusCode;
    private final String message;

    HandlestHttpStatus(int statusCode, String message) {
        this.statusCode = statusCode;
        this.message = message;
    }

    public int getCode() {
        return statusCode;
    }

    public String getMessage() {
        return message;
    }
}
