package company.vk.edu.distrib.compute.nesterukia.utils;

public class ClusterUtils {
    private static String LOCALHOST_PATTERN = "http://localhost:%d";

    public static String portToEndpoint(Integer port) {
        return LOCALHOST_PATTERN.formatted(port);
    }
}
