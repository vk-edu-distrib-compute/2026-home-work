package company.vk.edu.distrib.compute.linempy.routing;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * ConsistentHashRouter — описание класса.
 *
 * <p>
 * TODO: добавить описание назначения и поведения класса.
 * </p>
 *
 * @author Linempy
 * @since 17.04.2026
 */
public class ConsistentHashRouter implements ShardingStrategy {
    private final SortedMap<Long, String> ring = new TreeMap<>();

    private static final int VIRTUAL_NODES = 150;

    public ConsistentHashRouter(List<String> nodes) {
        buildRing(nodes);
    }

    private long hash(String key) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(key.getBytes(StandardCharsets.UTF_8));
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

    private void buildRing(List<String> nodes) {
        for (String node : nodes) {
            for (int i = 0; i < VIRTUAL_NODES; i++) {
                String virtualNode = node + "#" + i;
                long position = hash(virtualNode);
                ring.put(position, node);
            }
        }
    }

    @Override
    public String route(String key, List<String> nodes) {
        if (ring.isEmpty()) {
            throw new IllegalStateException("No nodes available");
        }

        long keyHash = hash(key);

        SortedMap<Long, String> tailMap = ring.tailMap(keyHash);

        if (tailMap.isEmpty()) {
            return ring.get(ring.firstKey());
        }

        return tailMap.get(tailMap.firstKey());
    }
}
