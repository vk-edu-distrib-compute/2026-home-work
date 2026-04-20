package company.vk.edu.distrib.compute.solntseva_nastya;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;

/**
 * Rendezvous (HRW) hashing router.
 *
 * <p>Assigns each key to the node with the highest hash(key + node).
 */
public final class SolnRendezvousHashRouter implements Router {

    private final Collection<String> endpoints;

    /**
     * Creates the router for the given set of node endpoints.
     *
     * @param endpoints node URLs
     */
    public SolnRendezvousHashRouter(Collection<String> endpoints) {
        this.endpoints = endpoints;
    }

    /**
     * Returns the node responsible for the given key.
     *
     * @param key the lookup key
     * @return the responsible node URL
     */
    @Override
    public String getNode(String key) {
        String bestNode = null;
        int maxHash = Integer.MIN_VALUE;

        for (String node : endpoints) {
            int currentHash = hash(key + node);
            if (currentHash > maxHash) {
                maxHash = currentHash;
                bestNode = node;
            }
        }
        return bestNode;
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
            throw new IllegalStateException("MD5 hash algorithm not found", ex);
        }
    }
}
