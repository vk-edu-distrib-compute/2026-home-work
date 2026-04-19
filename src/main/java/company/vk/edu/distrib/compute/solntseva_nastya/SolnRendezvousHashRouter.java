package company.vk.edu.distrib.compute.solntseva_nastya;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;

public final class SolnRendezvousHashRouter {

    private final Collection<String> endpoints;

    public SolnRendezvousHashRouter(Collection<String> endpoints) {
        this.endpoints = endpoints;
    }

    public String getNode(String key) {
        String bestNode = null;
        int maxHash = Integer.MIN_VALUE;

        for (String node : endpoints) {
            // Вес ноды — это хэш от комбинации ключа и URL ноды
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
            return ((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16) 
                 | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 not found", e);
        }
    }
}
