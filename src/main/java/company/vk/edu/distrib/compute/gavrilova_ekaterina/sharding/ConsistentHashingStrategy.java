package company.vk.edu.distrib.compute.gavrilova_ekaterina.sharding;

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHashingStrategy implements HashingStrategy {

    private static final long KNUTH_HASH_CONSTANT = 2654435761L;
    private static final int VIRTUAL_NODE_COUNT = 100;
    private final SortedMap<Long, String> ring = new TreeMap<>();

    @Override
    public void setEndpoints(List<String> endpoints) {
        ring.clear();

        for (String endpoint : endpoints) {
            for (int i = 0; i < VIRTUAL_NODE_COUNT; i++) {
                long hash = hash(endpoint + "#" + i);
                ring.put(hash, endpoint);
            }
        }
    }

    @Override
    public String getNode(String key) {
        if (ring.isEmpty()) {
            throw new IllegalStateException("Cluster has no nodes");
        }

        long hash = hash(key);

        SortedMap<Long, String> tailMap = ring.tailMap(hash);
        Long targetHash = tailMap.isEmpty()
                ? ring.firstKey()
                : tailMap.firstKey();

        return ring.get(targetHash);
    }

    private long hash(String s) {
        return s.hashCode() * KNUTH_HASH_CONSTANT;
    }

}
