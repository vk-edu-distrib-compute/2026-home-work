package company.vk.edu.distrib.compute.dariaprindina;

enum DPShardingAlgorithm {
    MODULO,
    RENDEZVOUS;

    static DPShardingAlgorithm fromProperty(String value) {
        if (value == null) {
            return MODULO;
        }
        final String normalized = value.trim().toLowerCase();
        if ("rendezvous".equals(normalized)) {
            return RENDEZVOUS;
        }
        return MODULO;
    }
}
