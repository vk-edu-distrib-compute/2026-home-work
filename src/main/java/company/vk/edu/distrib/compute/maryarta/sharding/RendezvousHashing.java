package company.vk.edu.distrib.compute.maryarta.sharding;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class RendezvousHashing implements ShardingStrategy {
    private final List<String> endpoints;

    public RendezvousHashing(List<String> endpoints) {
        this.endpoints = endpoints;
    }

    @Override
    public String getEndpoint(String key) {
        long max = Long.MIN_VALUE;
        String target = "";
        for (String endpoint: endpoints) {
            long score = hash(key + endpoint);
            if (score > max) {
                max = score;
                target = endpoint;
            }
        }
        return target;
    }

    private long hash(String value) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(value.getBytes());
            return ByteBuffer.wrap(digest).getLong();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm is not available", e);
        }
    }
}
