package company.vk.edu.distrib.compute.andrey1af.sharding;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class RendezvousHash implements HashRouter {
    private static final String HASH_ALGORITHM = "MD5";
    private final List<String> endpoints;

    public RendezvousHash(List<String> endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalArgumentException("endpoints must not be null or empty");
        }
        this.endpoints = List.copyOf(endpoints);
    }

    private static long calculateWeight(String key, String endpoint) {
        try {
            MessageDigest digest = MessageDigest.getInstance(HASH_ALGORITHM);
            byte[] hash = digest.digest((key + ':' + endpoint).getBytes(StandardCharsets.UTF_8));

            long value = 0;
            for (int i = 0; i < Long.BYTES; i++) {
                value = (value << Byte.SIZE) | (hash[i] & 0xffL);
            }
            return value;
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 algorithm is not available", e);
        }
    }

    @Override
    public String getEndpoint(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("key must not be null or blank");
        }

        String selectedEndpoint = null;
        long maxWeight = Long.MIN_VALUE;

        for (String endpoint : endpoints) {
            long weight = calculateWeight(key, endpoint);
            if (weight > maxWeight) {
                maxWeight = weight;
                selectedEndpoint = endpoint;
            }
        }

        return selectedEndpoint;
    }
}
