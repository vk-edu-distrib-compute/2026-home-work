package company.vk.edu.distrib.compute.handlest.routing;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Rendezvous (Highest Random Weight) hashing router.
 * For each key, assigns a score to every node as hash(node + key),
 * then picks the node with the highest score. This gives:
 * - deterministic assignment for the same set of nodes
 * - minimal redistribution when nodes are added/removed
 */
public class HandlestRendezvousRouter {

    private final List<String> endpoints;

    public HandlestRendezvousRouter(List<String> endpoints) {
        this.endpoints = new ArrayList<>(endpoints);
    }

    /**
     * Returns the endpoint responsible for the given key.
     */
    public String route(String key) {
        String best = null;
        long bestScore = Long.MIN_VALUE;

        for (String endpoint : endpoints) {
            long score = score(endpoint, key);
            if (score > bestScore) {
                bestScore = score;
                best = endpoint;
            }
        }

        return best;
    }

    private long score(String endpoint, String key) {
        // Combine node + key so every (node, key) pair gets a unique hash
        String combined = endpoint + '\0' + key;
        return murmurMix64(combined.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * A simple, avalanche-quality 64-bit hash mix based on MurmurHash3 finalizer.
     */
    private static long murmurMix64(byte[] data) {
        long h = 0xdeadbeef_cafebabeL;
        for (byte b : data) {
            h ^= Byte.toUnsignedLong(b);
            h *= 0xff51afd7ed558ccdL;
            h ^= h >>> 33;
            h *= 0xc4ceb9fe1a85ec53L;
            h ^= h >>> 33;
        }
        return h;
    }
}
