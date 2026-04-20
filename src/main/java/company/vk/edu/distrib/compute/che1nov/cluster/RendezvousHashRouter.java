package company.vk.edu.distrib.compute.che1nov.cluster;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
        if (Objects.isNull(key) || key.isBlank()) {
            throw new IllegalArgumentException("key must not be null or blank");
        }

        String selectedEndpoint = null;
        long selectedScore = Long.MIN_VALUE;

        for (String endpoint : endpoints) {
            long score = hashToLong(key + "|" + endpoint);
            if (score > selectedScore) {
                selectedScore = score;
                selectedEndpoint = endpoint;
            }
        }

        return selectedEndpoint;
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
}
