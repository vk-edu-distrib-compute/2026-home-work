package company.vk.edu.distrib.compute.vitos23.util;

public final class HttpUtils {
    public static final int NO_BODY_RESPONSE_LENGTH = -1;

    private HttpUtils() {
    }

    public static String getLocalEndpoint(int port) {
        return "http://localhost:" + port;
    }
}
