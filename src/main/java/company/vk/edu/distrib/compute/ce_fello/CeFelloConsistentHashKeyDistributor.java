package company.vk.edu.distrib.compute.ce_fello;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

final class CeFelloConsistentHashKeyDistributor implements CeFelloKeyDistributor {
    private static final int DEFAULT_VIRTUAL_NODE_COUNT = 128;

    private final List<RingEntry> ring;

    CeFelloConsistentHashKeyDistributor(Iterable<String> endpoints) {
        this(endpoints, DEFAULT_VIRTUAL_NODE_COUNT);
    }

    CeFelloConsistentHashKeyDistributor(Iterable<String> endpoints, int virtualNodeCount) {
        if (virtualNodeCount <= 0) {
            throw new IllegalArgumentException("virtualNodeCount must be positive");
        }

        this.ring = buildRing(endpoints, virtualNodeCount);
    }

    @Override
    public String ownerFor(String key) {
        validateKey(key);

        long keyHash = CeFelloHashing.hash64(key);
        int index = findRingIndex(keyHash);
        return ring.get(index).endpoint;
    }

    private int findRingIndex(long keyHash) {
        int left = 0;
        int right = ring.size();

        while (left < right) {
            int mid = (left + right) >>> 1;
            if (Long.compareUnsigned(ring.get(mid).hash, keyHash) < 0) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        return left == ring.size() ? 0 : left;
    }

    private static List<RingEntry> buildRing(Iterable<String> endpoints, int virtualNodeCount) {
        Objects.requireNonNull(endpoints, "endpoints");

        List<RingEntry> result = new ArrayList<>();
        for (String endpoint : endpoints) {
            if (endpoint == null || endpoint.isBlank()) {
                throw new IllegalArgumentException("endpoint must be non-empty");
            }
            for (int virtualNode = 0; virtualNode < virtualNodeCount; virtualNode++) {
                String token = endpoint + '#' + virtualNode;
                result.add(new RingEntry(CeFelloHashing.hash64(token), endpoint, virtualNode));
            }
        }

        if (result.isEmpty()) {
            throw new IllegalArgumentException("Missing endpoints for distribution");
        }

        result.sort((left, right) -> {
            int hashComparison = Long.compareUnsigned(left.hash, right.hash);
            if (hashComparison != 0) {
                return hashComparison;
            }

            int endpointComparison = left.endpoint.compareTo(right.endpoint);
            if (endpointComparison != 0) {
                return endpointComparison;
            }

            return Integer.compare(left.virtualNode, right.virtualNode);
        });
        return List.copyOf(result);
    }

    private static void validateKey(String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("key must be non-empty");
        }
    }

    private static final class RingEntry {
        private final long hash;
        private final String endpoint;
        private final int virtualNode;

        private RingEntry(long hash, String endpoint, int virtualNode) {
            this.hash = hash;
            this.endpoint = endpoint;
            this.virtualNode = virtualNode;
        }
    }
}
