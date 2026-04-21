package company.vk.edu.distrib.compute.handlest.routing;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Rendezvous (Highest Random Weight).
 * Для каждого ключа задает результат как hash(node + key),
 * затем выбирает наибольший результат.
 */
public class HandlestRendezvousRouter {

    private final List<String> endpoints;

    public HandlestRendezvousRouter(List<String> endpoints) {
        this.endpoints = new ArrayList<>(endpoints);
    }

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
        String combined = endpoint + '\0' + key;
        return murmurMix64(combined.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 64-битный hash mix основанный на MurmurHash3 finalizer.
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
