package company.vk.edu.distrib.compute.marinchanka;

public record ClusterNode(String host, int port) {
    public ClusterNode {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("Host cannot be null or empty");
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535");
        }
    }

    public String getEndpoint() {
        return host + ":" + port;
    }

    public String getUrl() {
        return "http://" + host + ":" + port;
    }

    @Override
    public String toString() {
        return getEndpoint();
    }
}
