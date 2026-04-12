package company.vk.edu.distrib.compute.gavrilova_ekaterina.sharding;

import java.util.List;

public class RendezvousHashingStrategy {

    private static final long KNUTH_HASH_CONSTANT = 2654435761L;

    private List<String> endpoints = List.of();

    public void setEndpoints(List<String> endpoints) {
        this.endpoints = List.copyOf(endpoints);
    }

    public String getNode(String key) {
        if (endpoints.isEmpty()) {
            throw new IllegalStateException("Cluster has no nodes");
        }

        if (endpoints.size() == 1) {
            return endpoints.getFirst();
        }

        String bestNode = null;
        long bestScore = Long.MIN_VALUE;

        for (String endpoint : endpoints) {
            long score = hash(key + endpoint);
            if (score > bestScore) {
                bestScore = score;
                bestNode = endpoint;
            }
        }
        return bestNode;
    }

    private long hash(String s) {
        return s.hashCode() * KNUTH_HASH_CONSTANT;
    }

}
