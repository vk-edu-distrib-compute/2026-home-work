package company.vk.edu.distrib.compute.wedwincode.sharded;

import java.util.List;

public class RendezvousHashStrategy implements HashStrategy {
    private final List<String> endpoints;

    public RendezvousHashStrategy(List<String> endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalStateException("endpoints list is null or empty");
        }
        this.endpoints = endpoints;
    }

    @Override
    public String getEndpoint(String id) {
        long bestScore = Long.MIN_VALUE;
        String bestEndpoint = null;

        for (String endpoint: endpoints) {
            long score = hash(id + endpoint);
            if (score > bestScore) {
                bestEndpoint = endpoint;
                bestScore = score;
            }
        }

        return bestEndpoint;
    }
}
