package company.vk.edu.distrib.compute.ce_fello;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

final class CeFelloRendezvousKeyDistributor implements CeFelloKeyDistributor {
    private static final char DELIMITER = '\u0000';

    private final List<String> endpoints;

    CeFelloRendezvousKeyDistributor(Iterable<String> endpoints) {
        this.endpoints = copyEndpoints(endpoints);
    }

    @Override
    public String ownerFor(String key) {
        validateKey(key);

        String bestEndpoint = endpoints.getFirst();
        long bestScore = score(bestEndpoint, key);

        for (int i = 1; i < endpoints.size(); i++) {
            String currentEndpoint = endpoints.get(i);
            long currentScore = score(currentEndpoint, key);
            if (Long.compareUnsigned(currentScore, bestScore) > 0
                    || Long.compareUnsigned(currentScore, bestScore) == 0
                    && currentEndpoint.compareTo(bestEndpoint) < 0) {
                bestEndpoint = currentEndpoint;
                bestScore = currentScore;
            }
        }

        return bestEndpoint;
    }

    private static List<String> copyEndpoints(Iterable<String> endpoints) {
        Objects.requireNonNull(endpoints, "endpoints");

        List<String> result = new ArrayList<>();
        for (String endpoint : endpoints) {
            if (endpoint == null || endpoint.isBlank()) {
                throw new IllegalArgumentException("endpoint must be non-empty");
            }
            result.add(endpoint);
        }

        if (result.isEmpty()) {
            throw new IllegalArgumentException("Missing endpoints for distribution");
        }

        return List.copyOf(result);
    }

    private static long score(String endpoint, String key) {
        return CeFelloHashing.hash64(endpoint + DELIMITER + key);
    }

    private static void validateKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key must be non-empty");
        }
    }
}
