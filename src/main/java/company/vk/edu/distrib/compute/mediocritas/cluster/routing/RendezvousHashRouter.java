package company.vk.edu.distrib.compute.mediocritas.cluster.routing;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class RendezvousHashRouter extends AbstractHashRouter {
    private final List<String> nodes = new CopyOnWriteArrayList<>();

    public void addNode(String endpoint) {
        if (!nodes.contains(endpoint)) {
            nodes.add(endpoint);
        }
    }

    public void removeNode(String endpoint) {
        nodes.remove(endpoint);
    }

    public String getNodeForKey(String key) {
        if (nodes.isEmpty()) {
            throw new IllegalStateException("No nodes available");
        }

        String selectedNode = null;
        long maxHash = Long.MIN_VALUE;

        for (String node : nodes) {
            long hash = hash(key + node);
            if (hash > maxHash) {
                maxHash = hash;
                selectedNode = node;
            }
        }

        return selectedNode;
    }

    public List<String> getAllNodes() {
        return new ArrayList<>(nodes);
    }
}
