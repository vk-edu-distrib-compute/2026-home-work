package company.vk.edu.distrib.compute.artsobol.impl;

record ReplicaStats(
        int replicaId,
        boolean enabled,
        int totalKeys,
        int liveKeys,
        int tombstones,
        long bytes
) {
    String toJson() {
        return """
                {"replicaId":%d,"enabled":%s,"totalKeys":%d,"liveKeys":%d,"tombstones":%d,"bytes":%d}
                """.formatted(replicaId, enabled, totalKeys, liveKeys, tombstones, bytes);
    }
}
