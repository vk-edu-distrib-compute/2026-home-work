package company.vk.edu.distrib.compute.artsobol.router;

import java.util.List;

public class RendezvousShardRouter implements ShardRouter {
    private final List<String> endpoints;

    public RendezvousShardRouter(List<String> endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalArgumentException("endpoints must not be empty");
        }
        this.endpoints = List.copyOf(endpoints);
    }

    @Override
    public String getOwnerEndpoint(String key) {
        if (key == null) {
            throw new IllegalArgumentException("key must not be null");
        }

        String owner = null;
        long bestScore = 0L;
        for (String endpoint : endpoints) {
            long score = Integer.toUnsignedLong((key + '\u0000' + endpoint).hashCode());
            if (owner == null || Long.compareUnsigned(score, bestScore) > 0) {
                owner = endpoint;
                bestScore = score;
            }
        }
        return owner;
    }
}
