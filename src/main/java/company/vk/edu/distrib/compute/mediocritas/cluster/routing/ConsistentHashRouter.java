package company.vk.edu.distrib.compute.mediocritas.cluster.routing;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class ConsistentHashRouter extends AbstractHashRouter {
    private final NavigableMap<Long, String> ring = new ConcurrentSkipListMap<>();
    private final int virtualNodesCount;

    public ConsistentHashRouter(int virtualNodesCount) {
        this.virtualNodesCount = virtualNodesCount;
    }

    public void addNode(String endpoint) {
        for (int i = 0; i < virtualNodesCount; i++) {
            String virtualNodeKey = endpoint + "#" + i;
            long hash = hash(virtualNodeKey);
            ring.put(hash, endpoint);
        }
    }

    public void removeNode(String endpoint) {
        for (int i = 0; i < virtualNodesCount; i++) {
            String virtualNodeKey = endpoint + "#" + i;
            long hash = hash(virtualNodeKey);
            ring.remove(hash);
        }
    }

    public String getNodeForKey(String key) {
        if (ring.isEmpty()) {
            throw new IllegalStateException("No nodes in the ring");
        }

        long hash = hash(key);
        Map.Entry<Long, String> entry = ring.ceilingEntry(hash);

        if (entry == null) {
            entry = ring.firstEntry();
        }

        return entry.getValue();
    }

    public List<String> getAllNodes() {
        return new ArrayList<>(ring.values()).stream().distinct().toList();
    }
}
