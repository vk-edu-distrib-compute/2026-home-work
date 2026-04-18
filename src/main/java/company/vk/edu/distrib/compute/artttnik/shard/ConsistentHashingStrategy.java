package company.vk.edu.distrib.compute.artttnik.shard;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

public class ConsistentHashingStrategy implements ShardingStrategy {

    private static final int VIRTUAL_NODES = 150;

    private volatile List<String> cachedEndpoints = List.of();
    private volatile NavigableMap<Long, String> ring = new TreeMap<>();

    @Override
    public String resolveOwner(String key, List<String> endpoints) {
        NavigableMap<Long, String> currentRing = getRing(endpoints);
        long keyHash = fnv1a(key.getBytes(StandardCharsets.UTF_8));
        NavigableMap<Long, String> tail = currentRing.tailMap(keyHash, true);
        return tail.isEmpty() ? currentRing.firstEntry().getValue() : tail.firstEntry().getValue();
    }

    private NavigableMap<Long, String> getRing(List<String> endpoints) {
        if (endpoints.equals(cachedEndpoints)) {
            return ring;
        }
        synchronized (this) {
            if (!endpoints.equals(cachedEndpoints)) {
                ring = buildRing(endpoints);
                cachedEndpoints = new ArrayList<>(endpoints);
            }
        }
        return ring;
    }

    private static NavigableMap<Long, String> buildRing(List<String> endpoints) {
        NavigableMap<Long, String> newRing = new TreeMap<>();
        for (String endpoint : endpoints) {
            for (int v = 0; v < VIRTUAL_NODES; v++) {
                byte[] bytes = (endpoint + '#' + v).getBytes(StandardCharsets.UTF_8);
                newRing.putIfAbsent(fnv1a(bytes), endpoint);
            }
        }
        return newRing;
    }

    private static long fnv1a(byte[] bytes) {
        long hash = 0xcbf29ce484222325L;
        for (byte b : bytes) {
            hash ^= (b & 0xFF);
            hash *= 0x100000001b3L;
        }
        return hash;
    }
}
