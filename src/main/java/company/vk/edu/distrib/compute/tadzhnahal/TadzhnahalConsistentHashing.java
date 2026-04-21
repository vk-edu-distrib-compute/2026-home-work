package company.vk.edu.distrib.compute.tadzhnahal;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

public class TadzhnahalConsistentHashing implements TadzhnahalShardSelector {
    private static final int VIRTUAL_NODE_COUNT = 128;

    private final List<String> endpoints;
    private final NavigableMap<Long, String> ring;

    public TadzhnahalConsistentHashing(List<String> endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalArgumentException("Endpoints must not be empty");
        }

        this.endpoints = List.copyOf(endpoints);
        this.ring = new TreeMap<>();
        buildRing();
    }

    @Override
    public String select(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key must not be empty");
        }

        long hash = hash(key);
        var entry = ring.ceilingEntry(hash);

        if (entry != null) {
            return entry.getValue();
        }

        return ring.firstEntry().getValue();
    }

    private void buildRing() {
        for (String endpoint : endpoints) {
            for (int i = 0; i < VIRTUAL_NODE_COUNT; i++) {
                long hash = hash(endpoint + "#virtual-node#" + i);
                ring.put(hash, endpoint);
            }
        }
    }

    private long hash(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        long result = 1125899906842597L;

        for (byte current : bytes) {
            result = 31 * result + current;
        }

        return result & Long.MAX_VALUE;
    }
}

