package company.vk.edu.distrib.compute.kruchinina.sharding;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Comparator;
import java.util.List;

/**
 * Реализация рандеву-хеширования.
 */
public class RendezvousHashingStrategy implements ShardingStrategy {

    @Override
    public String selectNode(String key, List<String> nodes) {
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("Cluster nodes list is empty");
        }
        //чем больше вес, тем лучше узел для данного ключа
        return nodes.stream()
                .max(Comparator.comparingLong(node -> hash(key + node)))
                .orElseThrow();
    }

    private long hash(String value) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-1");
            byte[] digest = md.digest(value.getBytes(StandardCharsets.UTF_8));
            long h = 0;
            for (int i = 0; i < 8; i++) {
                h <<= 8;
                h |= (digest[i] & 0xFF);
            }
            return h;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-1 algorithm not available", e);
        }
    }
}
