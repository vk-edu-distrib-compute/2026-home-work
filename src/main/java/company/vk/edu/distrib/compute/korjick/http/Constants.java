package company.vk.edu.distrib.compute.korjick.http;

public class Constants {
    public static final int EMPTY_BODY_LENGTH = -1;
    public static final String QUERY_PARAM_SEPARATOR = "&";
    public static final String QUERY_VALUE_SEPARATOR = "=";
    public static final String EMPTY_QUERY_PARAM_VALUE = "";

    public static final String HTTP_METHOD_GET = "GET";
    public static final String HTTP_METHOD_PUT = "PUT";
    public static final String HTTP_METHOD_DELETE = "DELETE";
    public static final String INTERNAL_REQUEST_HEADER = "X-Internal-Request";
    public static final String INTERNAL_REQUEST_HEADER_VALUE = "true";

    public static final int HTTP_STATUS_OK = 200;
    public static final int HTTP_STATUS_CREATED = 201;
    public static final int HTTP_STATUS_ACCEPTED = 202;
    public static final int HTTP_STATUS_BAD_REQUEST = 400;
    public static final int HTTP_STATUS_NOT_FOUND = 404;
    public static final int HTTP_STATUS_METHOD_NOT_ALLOWED = 405;
    public static final int HTTP_STATUS_INTERNAL_ERROR = 500;
    public static final int HTTP_STATUS_BAD_GATEWAY = 502;
}
