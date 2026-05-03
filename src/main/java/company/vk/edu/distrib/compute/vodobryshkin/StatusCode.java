package company.vk.edu.distrib.compute.vodobryshkin;

public enum StatusCode {
    OK(200),
    CREATED(201),
    ACCEPTED(202),
    BAD_REQUEST(400),
    NOT_FOUND(404),
    METHOD_NOT_ALLOWED(405),
    UNPROCESSABLE_CONTENT(422),
    SERVICE_UNAVAILABLE(503);

    private final int code;

    StatusCode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }
}
