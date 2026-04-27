package company.vk.edu.distrib.compute.golubtsov_pavel;

import java.util.List;
import java.util.Objects;

public class ShardResolver {
    private final List<String> endpoints;

    public ShardResolver(List<String> endpoints) {
        if ((endpoints == null) || (endpoints.isEmpty())) {
            throw new IllegalArgumentException("endpoints are empty or null");
        }
        this.endpoints = List.copyOf(endpoints);
    }

    public String resolve(String key) {
        if ((key == null) || (key.isBlank())) {
            throw new IllegalArgumentException("key is null or blank");
        }

        String bestEndpoint = null;
        long bestScore = Long.MIN_VALUE;

        for (String endpoint : endpoints) {
            long score = score(key, endpoint);
            if (score > bestScore) {
                bestScore = score;
                bestEndpoint = endpoint;
            }
        }
        return bestEndpoint;
    }

    public long score(String key, String endpoint) {
        return Objects.hash(key, endpoint);
    }

}



