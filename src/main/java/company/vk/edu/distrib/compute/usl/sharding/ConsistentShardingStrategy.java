package company.vk.edu.distrib.compute.usl.sharding;

import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

public final class ConsistentShardingStrategy implements ShardingStrategy {
    public static final int DEFAULT_VIRTUAL_NODE_COUNT = 128;

    private static final Comparator<Long> UNSIGNED_LONG_COMPARATOR = Long::compareUnsigned;

    private final List<String> endpointUrls;
    private final NavigableMap<Long, String> ring;

    public ConsistentShardingStrategy(List<String> endpoints) {
        this(endpoints, DEFAULT_VIRTUAL_NODE_COUNT);
    }

    public ConsistentShardingStrategy(List<String> endpoints, int virtualNodeCount) {
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalArgumentException("Endpoints must not be empty");
        }
        if (virtualNodeCount <= 0) {
            throw new IllegalArgumentException("Virtual node count must be positive");
        }
        this.endpointUrls = List.copyOf(endpoints);
        this.ring = new TreeMap<>(UNSIGNED_LONG_COMPARATOR);
        populateRing(virtualNodeCount);
    }

    @Override
    public String resolveOwner(String key) {
        Objects.requireNonNull(key, "key");
        long keyHash = HashSupport.hash64(key);
        var candidate = ring.ceilingEntry(keyHash);
        return candidate == null ? ring.firstEntry().getValue() : candidate.getValue();
    }

    @Override
    public List<String> endpoints() {
        return endpointUrls;
    }

    private void populateRing(int virtualNodeCount) {
        for (String endpoint : endpointUrls) {
            for (int vnode = 0; vnode < virtualNodeCount; vnode++) {
                long hash = HashSupport.hash64(endpoint, Integer.toString(vnode));
                while (ring.putIfAbsent(hash, endpoint) != null) {
                    hash++;
                }
            }
        }
    }
}
