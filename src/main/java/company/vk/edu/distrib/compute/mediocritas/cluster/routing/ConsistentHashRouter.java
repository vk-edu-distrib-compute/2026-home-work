package company.vk.edu.distrib.compute.mediocritas.cluster.routing;

import company.vk.edu.distrib.compute.mediocritas.cluster.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class ConsistentHashRouter extends AbstractHashRouter {

    private final NavigableMap<Long, Node> ring = new ConcurrentSkipListMap<>();
    private final Set<Node> uniqueNodes = ConcurrentHashMap.newKeySet();
    private final int virtualNodesCount;

    public ConsistentHashRouter(int virtualNodesCount) {
        super();
        this.virtualNodesCount = virtualNodesCount;
    }

    @Override
    public void addNode(Node node) {
        uniqueNodes.add(node);
        for (int i = 0; i < virtualNodesCount; i++) {
            String virtualNodeKey = node.httpEndpoint() + "#" + i;
            long hash = hash(virtualNodeKey);
            ring.put(hash, node);
        }
    }

    @Override
    public void removeNode(Node node) {
        uniqueNodes.remove(node);
        for (int i = 0; i < virtualNodesCount; i++) {
            String virtualNodeKey = node.httpEndpoint() + "#" + i;
            long hash = hash(virtualNodeKey);
            ring.remove(hash);
        }
    }

    @Override
    public Node getNodeForKey(String key) {
        if (ring.isEmpty()) {
            throw new IllegalStateException("No nodes in the ring");
        }
        long hash = hash(key);
        Map.Entry<Long, Node> entry = ring.ceilingEntry(hash);
        if (entry == null) {
            entry = ring.firstEntry();
        }
        return entry.getValue();
    }

    @Override
    public List<Node> getAllNodes() {
        return new ArrayList<>(uniqueNodes);
    }
}
