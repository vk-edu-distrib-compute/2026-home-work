package company.vk.edu.distrib.compute.linempy.routing;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

/**
 * RendezvousHashRouter — описание класса.
 *
 * <p>
 * TODO: добавить описание назначения и поведения класса.
 * </p>
 *
 * @author Linempy
 * @since 17.04.2026
 */
public class RendezvousHashRouter implements ShardingStrategy {

    private long hash(String node, String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            String combined = node + ":" + key;
            byte[] digest = md.digest(combined.getBytes(StandardCharsets.UTF_8));
            long hash = 0;
            for (int i = 0; i < 8; i++) {
                hash <<= 8;
                hash |= (digest[i] & 0xFF);
            }
            return hash & Long.MAX_VALUE;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String route(String key, List<String> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("No nodes available");
        }

        String bestNode = null;
        long maxHash = Long.MIN_VALUE;

        for (String node : nodes) {
            long score = hash(node, key);
            if (score > maxHash) {
                maxHash = score;
                bestNode = node;
            }
        }

        return bestNode;
    }
}
