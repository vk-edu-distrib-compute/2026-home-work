package company.vk.edu.distrib.compute.artsobol.impl;

record ReplicaAccessStats(int replicaId, long reads, long writes, long deletes) {
    String toJson() {
        return """
                {"replicaId":%d,"reads":%d,"writes":%d,"deletes":%d}
                """.formatted(replicaId, reads, writes, deletes);
    }
}
