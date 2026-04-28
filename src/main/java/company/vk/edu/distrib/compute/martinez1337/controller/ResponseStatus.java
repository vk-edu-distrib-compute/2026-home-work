package company.vk.edu.distrib.compute.martinez1337.controller;

public enum ResponseStatus {
    OK(200, "Success"),
    CREATED(201, "Created"),
    ACCEPTED(202, "Accepted"),
    BAD_REQUEST(400, "Invalid request parameters"),
    NOT_FOUND(404, "Resource not found"),
    METHOD_NOT_ALLOWED(405, "Method not allowed"),
    INTERNAL_ERROR(500, "Internal server error"),
    SERVICE_UNAVAILABLE(503, "Service is temporarily unavailable");

    private final int code;
    private final String message;

    ResponseStatus(int code, String message) {
        this.code = code;
        this.message = message;
    }

    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }
}
