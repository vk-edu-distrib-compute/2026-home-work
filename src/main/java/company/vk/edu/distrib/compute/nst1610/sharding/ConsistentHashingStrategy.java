package company.vk.edu.distrib.compute.nst1610.sharding;

import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

public class ConsistentHashingStrategy implements HashingStrategy {
    private static final int VIRTUAL_NODES = 128;
    private final NavigableMap<Long, String> ring = new TreeMap<>();

    public ConsistentHashingStrategy(List<String> endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalArgumentException("Endpoints must not be empty");
        }
        for (String endpoint : endpoints) {
            addEndpoint(endpoint);
        }
    }

    @Override
    public String resolve(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key must not be empty");
        }
        long keyHash = hash(key);
        var entry = ring.ceilingEntry(keyHash);
        if (entry == null) {
            entry = ring.firstEntry();
        }
        return entry.getValue();
    }

    private void addEndpoint(String endpoint) {
        for (int i = 0; i < VIRTUAL_NODES; i++) {
            long hash = (endpoint + "#" + i).hashCode();
            ring.put(hash, endpoint);
        }
    }

    private static long hash(String key) {
        return key.hashCode();
    }
}
