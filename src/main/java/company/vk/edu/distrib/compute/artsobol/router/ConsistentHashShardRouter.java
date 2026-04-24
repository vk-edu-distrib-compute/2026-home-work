package company.vk.edu.distrib.compute.artsobol.router;

import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

public class ConsistentHashShardRouter implements ShardRouter {
    private static final int VIRTUAL_NODES_PER_ENDPOINT = 128;

    private final NavigableMap<Long, String> ring = new TreeMap<>();

    public ConsistentHashShardRouter(List<String> endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalArgumentException("endpoints must not be empty");
        }

        for (String endpoint : endpoints) {
            for (int vnode = 0; vnode < VIRTUAL_NODES_PER_ENDPOINT; vnode++) {
                ring.put(Integer.toUnsignedLong((endpoint + "#" + vnode).hashCode()), endpoint);
            }
        }
    }

    @Override
    public String getOwnerEndpoint(String key) {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }

        long hash = Integer.toUnsignedLong(key.hashCode());
        var entry = ring.ceilingEntry(hash);
        if (entry == null) {
            entry = ring.firstEntry();
        }
        return entry.getValue();
    }
}
