package company.vk.edu.distrib.compute.usl.sharding;

import java.util.List;
import java.util.Locale;

public enum ShardingAlgorithm {
    RENDEZVOUS,
    CONSISTENT;

    public static ShardingAlgorithm fromExternalName(String value) {
        if (value == null || value.isBlank()) {
            return RENDEZVOUS;
        }

        return switch (value.trim().toLowerCase(Locale.ROOT)) {
            case "rendezvous" -> RENDEZVOUS;
            case "consistent" -> CONSISTENT;
            default -> throw new IllegalArgumentException("Unsupported sharding algorithm: " + value);
        };
    }

    public ShardingStrategy createStrategy(List<String> endpoints) {
        return switch (this) {
            case RENDEZVOUS -> new RendezvousShardingStrategy(endpoints);
            case CONSISTENT -> new ConsistentShardingStrategy(endpoints);
        };
    }
}
