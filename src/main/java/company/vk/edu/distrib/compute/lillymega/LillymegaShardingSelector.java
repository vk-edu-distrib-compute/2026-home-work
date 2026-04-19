package company.vk.edu.distrib.compute.lillymega;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

final class LillymegaShardingSelector {
    private static final String STRATEGY_PROPERTY = "lillymega.sharding";
    private static final String STRATEGY_RENDEZVOUS = "rendezvous";
    private static final String STRATEGY_CONSISTENT = "consistent";
    private static final int VIRTUAL_NODE_COUNT = 128;

    private final List<String> endpoints;
    private final Strategy strategy;
    private final NavigableMap<Long, String> ring;

    LillymegaShardingSelector(List<String> endpoints, Strategy strategy) {
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalArgumentException("Cluster endpoints must be not empty");
        }

        List<String> sortedEndpoints = new java.util.ArrayList<>(endpoints);
        Collections.sort(sortedEndpoints);
        this.endpoints = List.copyOf(sortedEndpoints);
        this.strategy = strategy;
        this.ring = strategy == Strategy.CONSISTENT ? buildRing(this.endpoints) : null;
    }

    static Strategy resolveStrategy() {
        String propertyValue = System.getProperty(STRATEGY_PROPERTY, STRATEGY_RENDEZVOUS);
        if (STRATEGY_CONSISTENT.equalsIgnoreCase(propertyValue)) {
            return Strategy.CONSISTENT;
        }

        return Strategy.RENDEZVOUS;
    }

    String selectEndpoint(String key) {
        if (strategy == Strategy.CONSISTENT) {
            return selectByConsistentHashing(key);
        }

        return selectByRendezvousHashing(key);
    }

    private String selectByRendezvousHashing(String key) {
        String bestEndpoint = null;
        long bestHash = Long.MIN_VALUE;

        for (String endpoint : endpoints) {
            long hash = hash(key + "#" + endpoint);
            if (hash > bestHash) {
                bestHash = hash;
                bestEndpoint = endpoint;
            }
        }

        return bestEndpoint;
    }

    private String selectByConsistentHashing(String key) {
        long hash = hash(key);
        var target = ring.ceilingEntry(hash);
        if (target != null) {
            return target.getValue();
        }

        return ring.firstEntry().getValue();
    }

    private static NavigableMap<Long, String> buildRing(List<String> endpoints) {
        NavigableMap<Long, String> result = new TreeMap<>();
        for (String endpoint : endpoints) {
            for (int nodeIndex = 0; nodeIndex < VIRTUAL_NODE_COUNT; nodeIndex++) {
                result.put(hash(endpoint + "#" + nodeIndex), endpoint);
            }
        }
        return result;
    }

    private static long hash(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        long hash = 0xcbf29ce484222325L;
        for (byte current : bytes) {
            hash ^= (current & 0xffL);
            hash *= 0x100000001b3L;
        }
        return hash;
    }

    enum Strategy {
        RENDEZVOUS,
        CONSISTENT
    }
}
