package company.vk.edu.distrib.compute.glekoz.cluster;

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHash {

    private static final long HASH_CONSTANT = 2654435769L;
    private static final int VNODE_COUNT = 10;
    private final SortedMap<Long, String> ring = new TreeMap<>();

    public ConsistentHash(List<String> endpoints) {
        ring.clear();

        for (String endpoint : endpoints) {
            for (int i = 0; i < VNODE_COUNT; i++) {
                long hash = hash(endpoint + i);
                ring.put(hash, endpoint);
            }
        }
    }

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
        return s.hashCode() * HASH_CONSTANT;
    }

}
