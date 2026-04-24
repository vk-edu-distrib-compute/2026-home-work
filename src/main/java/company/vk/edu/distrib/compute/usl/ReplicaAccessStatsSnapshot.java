package company.vk.edu.distrib.compute.usl;

record ReplicaAccessStatsSnapshot(
    int replicaId,
    boolean enabled,
    long reads,
    long writes,
    long deletes
) {
    String toJson() {
        return "{"
            + "\"replicaId\":" + replicaId + ','
            + "\"enabled\":" + enabled + ','
            + "\"reads\":" + reads + ','
            + "\"writes\":" + writes + ','
            + "\"deletes\":" + deletes
            + '}';
    }
}
