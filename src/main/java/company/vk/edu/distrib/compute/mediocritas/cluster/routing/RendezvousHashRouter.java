package company.vk.edu.distrib.compute.mediocritas.cluster.routing;

import company.vk.edu.distrib.compute.mediocritas.cluster.Node;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class RendezvousHashRouter extends AbstractHashRouter {

    private final List<Node> nodes = new CopyOnWriteArrayList<>();

    @Override
    public void addNode(Node node) {
        if (!nodes.contains(node)) {
            nodes.add(node);
        }
    }

    @Override
    public void removeNode(Node node) {
        nodes.remove(node);
    }

    @Override
    public Node getNodeForKey(String key) {
        if (nodes.isEmpty()) {
            throw new IllegalStateException("No nodes available");
        }
        Node selectedNode = null;
        long maxHash = Long.MIN_VALUE;
        for (Node node : nodes) {
            long hash = hash(key + node.httpEndpoint());
            if (hash > maxHash) {
                maxHash = hash;
                selectedNode = node;
            }
        }
        return selectedNode;
    }

    @Override
    public List<Node> getAllNodes() {
        return new ArrayList<>(nodes);
    }
}
