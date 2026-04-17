package company.vk.edu.distrib.compute.nst1610.sharding;

import java.util.List;

public class RendezvousStrategy {
    private final List<String> endpoints;

    public RendezvousStrategy(List<String> endpoints) {
        this.endpoints = List.copyOf(endpoints);
    }

    public String resolve(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key must not be empty");
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

    private static long hash(String key, String endpoint) {
        return (key + endpoint).hashCode();
    }
}
