package company.vk.edu.distrib.compute.artttnik;

public final class ReplicationConfig {
    private static final String ENV_REPLICA_COUNT = "ARTTTNIK_REPLICATION_FACTOR";
    private static final String PROPERTY_REPLICA_COUNT = "artttnik.replicationFactor";
    private static final String CLI_REPLICA_COUNT_PREFIX = "--replicas=";
    private static final int DEFAULT_REPLICA_COUNT = 3;

    private ReplicationConfig() {
    }

    public static int resolveReplicaCount(String... args) {
        Integer cliReplicaCount = parseCliReplicaCount(args);
        if (cliReplicaCount != null) {
            return cliReplicaCount;
        }

        return resolveReplicaCount();
    }

    public static int resolveReplicaCount() {
        String propertyValue = System.getProperty(PROPERTY_REPLICA_COUNT);
        if (propertyValue != null && !propertyValue.isBlank()) {
            return parseReplicaCount(propertyValue, "system property " + PROPERTY_REPLICA_COUNT);
        }

        String envValue = System.getenv(ENV_REPLICA_COUNT);
        if (envValue != null && !envValue.isBlank()) {
            return parseReplicaCount(envValue, "environment variable " + ENV_REPLICA_COUNT);
        }

        return DEFAULT_REPLICA_COUNT;
    }

    private static Integer parseCliReplicaCount(String... args) {
        if (args == null) {
            return null;
        }

        for (String arg : args) {
            if (arg != null && arg.startsWith(CLI_REPLICA_COUNT_PREFIX)) {
                return parseReplicaCount(arg.substring(CLI_REPLICA_COUNT_PREFIX.length()), arg);
            }
        }

        return null;
    }

    private static int parseReplicaCount(String value, String source) {
        try {
            int replicaCount = Integer.parseInt(value);
            if (replicaCount <= 0) {
                throw new IllegalArgumentException("Replica count must be positive: " + source);
            }
            return replicaCount;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid replica count from " + source + ": " + value, e);
        }
    }
}
