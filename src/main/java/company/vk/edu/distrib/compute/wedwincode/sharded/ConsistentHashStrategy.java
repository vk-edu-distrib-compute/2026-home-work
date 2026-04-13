package company.vk.edu.distrib.compute.wedwincode.sharded;

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHashStrategy implements HashStrategy {
    private final SortedMap<Long, String> ring = new TreeMap<>();

    public ConsistentHashStrategy(List<String> endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            throw new RuntimeException("endpoints list is null or empty");
        }
        endpoints.forEach((endpoint) -> ring.put(hash(endpoint), endpoint));
    }

    @Override
    public String getEndpoint(String id) {
        if (ring.isEmpty()) {
            return null;
        }

        SortedMap<Long, String> tail = ring.tailMap(hash(id));
        if (tail.isEmpty()) {
            return ring.get(ring.firstKey());
        } else {
            return tail.get(tail.firstKey());
        }
    }
}
