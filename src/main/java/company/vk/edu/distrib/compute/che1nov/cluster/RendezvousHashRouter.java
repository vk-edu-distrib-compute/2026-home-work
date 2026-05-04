package company.vk.edu.distrib.compute.che1nov.cluster;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class RendezvousHashRouter implements ShardRouter {
    private static final String HASH_ALGORITHM = "SHA-256";

    private final List<String> endpoints;

    public RendezvousHashRouter(List<String> endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalArgumentException("endpoints must not be null or empty");
        }

        this.endpoints = List.copyOf(endpoints);
    }

    @Override
    public String endpointByKey(String key) {
        return endpointsByKey(key, 1).get(0);
    }

    @Override
    public List<String> endpointsByKey(String key, int count) {
        if (Objects.isNull(key) || key.isBlank()) {
            throw new IllegalArgumentException("key must not be null or blank");
        }
        if (count <= 0) {
            throw new IllegalArgumentException("count must be positive");
        }
        if (count > endpoints.size()) {
            throw new IllegalArgumentException("count must not be greater than endpoints size");
        }

        List<EndpointScore> scores = new ArrayList<>(endpoints.size());
        for (String endpoint : endpoints) {
            long score = hashToLong(key + "|" + endpoint);
            scores.add(new EndpointScore(endpoint, score));
        }
        scores.sort(Comparator.comparingLong(EndpointScore::score).reversed());

        List<String> selectedEndpoints = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            selectedEndpoints.add(scores.get(i).endpoint());
        }
        return selectedEndpoints;
    }

    private static long hashToLong(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance(HASH_ALGORITHM);
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            long result = 0;
            for (int i = 0; i < Long.BYTES; i++) {
                result = (result << Byte.SIZE) | (hash[i] & 0xffL);
            }
            return result;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Missing hash algorithm: " + HASH_ALGORITHM, e);
        }
    }

    private record EndpointScore(String endpoint, long score) {
    }
}
