package company.vk.edu.distrib.compute.nihuaway00.task5;

public record NodeConfig(
        long pollTimeoutMs,
        long pingIntervalMs,
        long pingTimeoutMs,
        long electionTimeoutMs
) {
    public NodeConfig {
        if (pollTimeoutMs <= 0) {
            throw new IllegalArgumentException("pollTimeoutMs must be > 0");
        }
        if (pingIntervalMs <= 0) {
            throw new IllegalArgumentException("pingIntervalMs must be > 0");
        }
        if (pingTimeoutMs <= 0) {
            throw new IllegalArgumentException("pingTimeoutMs must be > 0");
        }
        if (electionTimeoutMs <= 0) {
            throw new IllegalArgumentException("electionTimeoutMs must be > 0");
        }
    }

    public static NodeConfig defaultConfig() {
        return new NodeConfig(100, 400, 1_200, 800);
    }
}
