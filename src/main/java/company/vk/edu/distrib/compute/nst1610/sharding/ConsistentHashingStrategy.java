package company.vk.edu.distrib.compute.nst1610.sharding;

import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

public class ConsistentHashingStrategy implements HashingStrategy {
    private static final int VIRTUAL_NODES = 128;

    private NavigableMap<Long, String> ring = new TreeMap<>();

    @Override
    public String resolve(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key must not be empty");
        }
        if (ring.isEmpty()) {
            throw new IllegalStateException("No active endpoints available");
        }
        long keyHash = hash(key);
        var entry = ring.ceilingEntry(keyHash);
        if (entry == null) {
            entry = ring.firstEntry();
        }
        return entry.getValue();
    }

    @Override
    public void updateEndpoints(List<String> endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalArgumentException("Endpoints must not be empty");
        }
        NavigableMap<Long, String> newRing = new TreeMap<>();
        for (String endpoint : endpoints) {
            addEndpoint(newRing, endpoint);
        }
        ring = newRing;
    }

    private void addEndpoint(NavigableMap<Long, String> ring, String endpoint) {
        for (int i = 0; i < VIRTUAL_NODES; i++) {
            long hash = hash(endpoint + "#" + i);
            ring.put(hash, endpoint);
        }
    }

    private static long hash(String value) {
        return value.hashCode();
    }
}
