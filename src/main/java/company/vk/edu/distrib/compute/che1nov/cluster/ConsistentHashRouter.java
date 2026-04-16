package company.vk.edu.distrib.compute.che1nov.cluster;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

public class ConsistentHashRouter implements ShardRouter {
    private static final String HASH_ALGORITHM = "SHA-256";
    private static final int VIRTUAL_NODES_PER_ENDPOINT = 128;

    private final NavigableMap<Long, String> ring;
    private final int endpointCount;

    public ConsistentHashRouter(List<String> endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalArgumentException("endpoints must not be null or empty");
        }

        this.ring = new TreeMap<>();
        this.endpointCount = endpoints.size();
        for (String endpoint : endpoints) {
            for (int i = 0; i < VIRTUAL_NODES_PER_ENDPOINT; i++) {
                long hash = hashToLong(endpoint + "#" + i);
                ring.put(hash, endpoint);
            }
        }
    }

    @Override
    public String endpointByKey(String key) {
        return endpointsByKey(key, 1).get(0);
    }

    @Override
    public List<String> endpointsByKey(String key, int count) {
        validateRequest(key, count);
        Map.Entry<Long, String> startEntry = startEntryForKey(key);
        Set<String> selected = collectDistinctEndpoints(startEntry, count);
        return new ArrayList<>(selected);
    }

    private void validateRequest(String key, int count) {
        if (Objects.isNull(key) || key.isBlank()) {
            throw new IllegalArgumentException("key must not be null or blank");
        }
        if (count <= 0) {
            throw new IllegalArgumentException("count must be positive");
        }
        if (count > endpointCount) {
            throw new IllegalArgumentException("count must not be greater than endpoints size");
        }
    }

    private Map.Entry<Long, String> startEntryForKey(String key) {
        long keyHash = hashToLong(key);
        Map.Entry<Long, String> entry = ring.ceilingEntry(keyHash);
        if (entry != null) {
            return entry;
        }

        Map.Entry<Long, String> firstEntry = ring.firstEntry();
        if (firstEntry == null) {
            throw new IllegalStateException("ring is empty");
        }
        return firstEntry;
    }

    private Set<String> collectDistinctEndpoints(Map.Entry<Long, String> startEntry, int count) {
        Set<String> selected = new LinkedHashSet<>();
        Map.Entry<Long, String> cursor = startEntry;

        while (selected.size() < count && selected.size() < endpointCount) {
            selected.add(cursor.getValue());
            cursor = nextEntry(cursor);
        }
        return selected;
    }

    private Map.Entry<Long, String> nextEntry(Map.Entry<Long, String> currentEntry) {
        Map.Entry<Long, String> nextEntry = ring.higherEntry(currentEntry.getKey());
        if (nextEntry != null) {
            return nextEntry;
        }

        Map.Entry<Long, String> firstEntry = ring.firstEntry();
        if (firstEntry == null) {
            throw new IllegalStateException("ring is empty");
        }
        return firstEntry;
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
