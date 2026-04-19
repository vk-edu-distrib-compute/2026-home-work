package company.vk.edu.distrib.compute.goshanchic;

import java.util.Collection;
import java.util.Comparator;

public final class RendezvousHasher {

    public String getTargetNode(String key, Collection<String> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalStateException("No nodes available");
        }

        return nodes.stream()
                .max(Comparator.comparingLong(node -> {
                    int h1 = key.hashCode();
                    int h2 = node.hashCode();
                    return ((long) h1 << 32) | (h2 & 0xFFFFFFFFL);
                }))
                .orElseThrow(() -> new IllegalStateException("No nodes available"));
    }
}
