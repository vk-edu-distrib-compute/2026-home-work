package company.vk.edu.distrib.compute.che1nov.cluster;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

public class ConsistentHashRouter implements ShardRouter {
    private static final String HASH_ALGORITHM = "SHA-256";
    private static final int VIRTUAL_NODES_PER_ENDPOINT = 128;

    private final NavigableMap<Long, String> ring;

    public ConsistentHashRouter(List<String> endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalArgumentException("endpoints must not be null or empty");
        }

        this.ring = new TreeMap<>();
        for (String endpoint : endpoints) {
            for (int i = 0; i < VIRTUAL_NODES_PER_ENDPOINT; i++) {
                long hash = hashToLong(endpoint + "#" + i);
                ring.put(hash, endpoint);
            }
        }
    }

    @Override
    public String endpointByKey(String key) {
        if (Objects.isNull(key) || key.isBlank()) {
            throw new IllegalArgumentException("key must not be null or blank");
        }

        long keyHash = hashToLong(key);
        Map.Entry<Long, String> entry = ring.ceilingEntry(keyHash);
        if (entry == null) {
            return ring.firstEntry().getValue();
        }

        return entry.getValue();
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
