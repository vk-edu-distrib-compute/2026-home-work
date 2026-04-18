package company.vk.edu.distrib.compute.dariaprindina;

import java.util.Locale;

enum DPShardingAlgorithm {
    MODULO,
    RENDEZVOUS;

    static DPShardingAlgorithm fromProperty(String value) {
        if (value == null) {
            return MODULO;
        }
        final String normalized = value.trim().toLowerCase(Locale.ROOT);
        if ("rendezvous".equals(normalized)) {
            return RENDEZVOUS;
        }
        return MODULO;
    }
}
