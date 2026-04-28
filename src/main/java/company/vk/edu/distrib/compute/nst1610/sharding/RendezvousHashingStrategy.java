package company.vk.edu.distrib.compute.nst1610.sharding;

import java.util.List;

public class RendezvousHashingStrategy implements HashingStrategy {
    private List<String> endpoints = List.of();

    @Override
    public String resolve(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key must not be empty");
        }
        if (endpoints.isEmpty()) {
            throw new IllegalStateException("No active endpoints available");
        }
        String bestEndpoint = null;
        long maxHash = Long.MIN_VALUE;
        for (String endpoint : endpoints) {
            long currentHash = hash(key, endpoint);
            if (currentHash > maxHash) {
                bestEndpoint = endpoint;
                maxHash = currentHash;
            }
        }
        return bestEndpoint;
    }

    @Override
    public void updateEndpoints(List<String> endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalArgumentException("Endpoints must not be empty");
        }
        this.endpoints = List.copyOf(endpoints);
    }

    private static long hash(String key, String endpoint) {
        return (key + endpoint).hashCode();
    }
}
