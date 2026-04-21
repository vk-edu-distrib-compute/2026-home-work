package company.vk.edu.distrib.compute.tadzhnahal;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

public class TadzhnahalRendezvousHashing {
    private static final String HASH_ALGORITHM = "SHA-256";

    private final List<String> endpoints;

    public TadzhnahalRendezvousHashing(List<String> endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalArgumentException("Endpoints must not be empty");
        }

        this.endpoints = new ArrayList<>(endpoints);
    }

    public String select(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key must not be empty");
        }

        String bestEndpoint = null;
        long bestScore = 0L;

        for (String endpoint : endpoints) {
            long score = score(endpoint, key);

            if (bestEndpoint == null || Long.compareUnsigned(score, bestScore) > 0) {
                bestEndpoint = endpoint;
                bestScore = score;
            }
        }

        return bestEndpoint;
    }

    private long score(String endpoint, String key) {
        MessageDigest digest = newDigest();
        byte[] hash = digest.digest((endpoint + '\n' + key).getBytes(StandardCharsets.UTF_8));

        long result = 0L;
        for (int i = 0; i < Long.BYTES; i++) {
            result = (result << 8) | (hash[i] & 0xffL);
        }

        return result;
    }

    private MessageDigest newDigest() {
        try {
            return MessageDigest.getInstance(HASH_ALGORITHM);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Hash algorithm is not available", e);
        }
    }
}
