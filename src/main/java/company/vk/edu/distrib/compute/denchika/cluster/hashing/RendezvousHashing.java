package company.vk.edu.distrib.compute.denchika.cluster.hashing;

import java.util.List;
import java.util.Objects;

public class RendezvousHashing implements DistributingAlgorithm {
    private final List<String> nodes;

    public RendezvousHashing(List<String> nodes) {
        this.nodes = List.copyOf(nodes);
    }

    @Override
    public String selectNode(String key) {
        if (nodes.isEmpty()) {
            throw new IllegalStateException("No nodes available");
        }
        String bestNode = nodes.get(0);
        int maxHash = hash(key, bestNode);
        for (int i = 1; i < nodes.size(); i++) {
            String node = nodes.get(i);
            int h = hash(key, node);
            if (h > maxHash) {
                maxHash = h;
                bestNode = node;
            }
        }
        return bestNode;
    }

    private int hash(String key, String node) {
        return Objects.hash(key, node);
    }
}
