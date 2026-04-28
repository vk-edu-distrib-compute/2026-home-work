package company.vk.edu.distrib.compute.usl;

record ReplicaStatsSnapshot(
    int replicaId,
    boolean enabled,
    int storedKeys,
    int tombstones,
    long storedBytes
) {
    String toJson() {
        return "{"
            + "\"replicaId\":" + replicaId + ','
            + "\"enabled\":" + enabled + ','
            + "\"storedKeys\":" + storedKeys + ','
            + "\"tombstones\":" + tombstones + ','
            + "\"storedBytes\":" + storedBytes
            + '}';
    }
}
