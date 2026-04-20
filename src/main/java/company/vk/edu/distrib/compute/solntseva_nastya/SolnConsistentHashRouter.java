package company.vk.edu.distrib.compute.solntseva_nastya;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Consistent hashing router.
 *
 * <p>Distributes keys across nodes using a virtual ring.
 */
public final class SolnConsistentHashRouter implements Router {

    private static final int VIRTUAL_NODES = 150;

    private final SortedMap<Integer, String> ring = new TreeMap<>();

    /**
     * Builds the hash ring from the given endpoints.
     *
     * @param endpoints list of node URLs
     */
    public SolnConsistentHashRouter(Iterable<String> endpoints) {
        for (String endpoint : endpoints) {
            addNode(endpoint);
        }
    }

    /**
     * Returns the node responsible for the given key.
     *
     * @param key the lookup key
     * @return the responsible node URL
     */
    @Override
    public String getNode(String key) {
        if (ring.isEmpty()) {
            return null;
        }
        int hash = hash(key);
        SortedMap<Integer, String> tail = ring.tailMap(hash);
        int nodeHash = tail.isEmpty() ? ring.firstKey() : tail.firstKey();
        return ring.get(nodeHash);
    }

    private void addNode(String endpoint) {
        for (int vn = 0; vn < VIRTUAL_NODES; vn++) {
            int hash = hash(endpoint + "#" + vn);
            ring.put(hash, endpoint);
        }
    }

    private static int hash(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] bytes = md.digest(input.getBytes(StandardCharsets.UTF_8));
            return ((bytes[0] & 0xFF) << 24)
                | ((bytes[1] & 0xFF) << 16)
                | ((bytes[2] & 0xFF) << 8)
                | (bytes[3] & 0xFF);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("MD5 not available", ex);
        }
    }
}
