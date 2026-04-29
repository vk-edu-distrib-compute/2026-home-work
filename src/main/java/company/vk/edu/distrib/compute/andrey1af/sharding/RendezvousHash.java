package company.vk.edu.distrib.compute.andrey1af.sharding;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RendezvousHash implements HashRouter {
    private static final String HASH_ALGORITHM = "MD5";
    private static final ThreadLocal<MessageDigest> DIGEST = ThreadLocal.withInitial(RendezvousHash::newDigest);
    private final List<String> endpoints;

    public RendezvousHash(List<String> endpoints) {
        validateEndpoints(endpoints);
        this.endpoints = List.copyOf(endpoints);
    }

    private static long calculateWeight(String key, String endpoint) {
        byte[] hash = DIGEST.get().digest((key + ':' + endpoint).getBytes(StandardCharsets.UTF_8));

        long value = 0;
        for (int i = 0; i < Long.BYTES; i++) {
            value = (value << Byte.SIZE) | (hash[i] & 0xffL);
        }
        return value;
    }

    private static MessageDigest newDigest() {
        try {
            return MessageDigest.getInstance(HASH_ALGORITHM);
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
            if (selectedEndpoint == null || Long.compareUnsigned(weight, maxWeight) > 0) {
                maxWeight = weight;
                selectedEndpoint = endpoint;
            }
        }

        return selectedEndpoint;
    }

    private static void validateEndpoints(List<String> endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalArgumentException("endpoints must not be null or empty");
        }

        Set<String> uniqueEndpoints = new HashSet<>();
        for (String endpoint : endpoints) {
            if (endpoint == null || endpoint.isBlank()) {
                throw new IllegalArgumentException("endpoint must not be null or blank");
            }
            if (!uniqueEndpoints.add(endpoint)) {
                throw new IllegalArgumentException("duplicate endpoint: " + endpoint);
            }
        }
    }
}
