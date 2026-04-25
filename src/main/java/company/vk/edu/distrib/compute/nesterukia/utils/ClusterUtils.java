package company.vk.edu.distrib.compute.nesterukia.utils;

public final class ClusterUtils {
    private static final String LOCALHOST_PATTERN = "http://localhost:%d";

    public static String portToEndpoint(Integer port) {
        return LOCALHOST_PATTERN.formatted(port);
    }

    private ClusterUtils() {
        // cannot have instances
    }
}
