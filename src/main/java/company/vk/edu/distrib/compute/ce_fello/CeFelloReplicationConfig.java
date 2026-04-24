package company.vk.edu.distrib.compute.ce_fello;

final class CeFelloReplicationConfig {
    private static final int DEFAULT_REPLICATION_FACTOR = 3;
    private static final int DEFAULT_STORAGE_REPLICAS = 6;

    private final int replicationFactor;
    private final int totalReplicas;

    private CeFelloReplicationConfig(int replicationFactor, int totalReplicas) {
        this.replicationFactor = replicationFactor;
        this.totalReplicas = totalReplicas;
    }

    static CeFelloReplicationConfig fromSystemProperties() {
        int replicationFactor = Integer.getInteger("ce_fello.replication.factor", DEFAULT_REPLICATION_FACTOR);
        int totalReplicas = Integer.getInteger("ce_fello.replication.nodes", DEFAULT_STORAGE_REPLICAS);
        if (replicationFactor < 1) {
            throw new IllegalArgumentException("Replication factor must be positive");
        }
        if (totalReplicas < replicationFactor) {
            throw new IllegalArgumentException("Replica nodes count must be >= replication factor");
        }
        return new CeFelloReplicationConfig(replicationFactor, totalReplicas);
    }

    int replicationFactor() {
        return replicationFactor;
    }

    int totalReplicas() {
        return totalReplicas;
    }
}
