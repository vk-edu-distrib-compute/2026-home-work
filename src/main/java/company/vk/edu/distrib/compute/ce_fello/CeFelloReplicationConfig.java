package company.vk.edu.distrib.compute.ce_fello;

final class CeFelloReplicationConfig {
    private static final int DEFAULT_REPLICATION_FACTOR = 3;
    private static final int DEFAULT_STORAGE_REPLICAS = 6;
    private static final int MIN_REPLICATION_FACTOR = 1;

    private final int configuredReplicationFactor;
    private final int configuredTotalReplicas;

    private CeFelloReplicationConfig(int replicationFactor, int totalReplicas) {
        this.configuredReplicationFactor = replicationFactor;
        this.configuredTotalReplicas = totalReplicas;
    }

    static CeFelloReplicationConfig fromSystemProperties() {
        int replicationFactor = Integer.getInteger("ce_fello.replication.factor", DEFAULT_REPLICATION_FACTOR);
        int totalReplicas = Integer.getInteger("ce_fello.replication.nodes", DEFAULT_STORAGE_REPLICAS);
        if (replicationFactor < MIN_REPLICATION_FACTOR) {
            throw new IllegalArgumentException("Replication factor must be positive");
        }
        if (totalReplicas < replicationFactor) {
            throw new IllegalArgumentException("Replica nodes count must be >= replication factor");
        }
        return new CeFelloReplicationConfig(replicationFactor, totalReplicas);
    }

    int replicationFactor() {
        return configuredReplicationFactor;
    }

    int totalReplicas() {
        return configuredTotalReplicas;
    }
}
