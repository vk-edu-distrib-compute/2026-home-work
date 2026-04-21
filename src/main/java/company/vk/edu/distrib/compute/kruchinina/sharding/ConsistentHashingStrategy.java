package company.vk.edu.distrib.compute.kruchinina.sharding;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Реализация последовательного хеширования с виртуальными узлами.
 */
public class ConsistentHashingStrategy implements ShardingStrategy {
    //количество виртуальных узлов, создаваемых для каждого физического узла
    private static final int VIRTUAL_NODES_PER_PHYSICAL = 100;
    private final SortedMap<Long, String> ring = new TreeMap<>();

    public ConsistentHashingStrategy(List<String> nodes) {
        rebuildRing(nodes);
    }

    private void rebuildRing(List<String> nodes) {
        ring.clear();
        for (String node : nodes) {
            for (int i = 0; i < VIRTUAL_NODES_PER_PHYSICAL; i++) {
                String virtualNode = node + "#" + i;
                long hash = hash(virtualNode);
                ring.put(hash, node);
            }
        }
    }

    @Override
    public String selectNode(String key, List<String> nodes) {
        if (nodes.isEmpty()) {
            throw new IllegalArgumentException("Cluster nodes list is empty");
        }
        rebuildRing(nodes);
        long hash = hash(key);
        if (!ring.containsKey(hash)) {
            SortedMap<Long, String> tailMap = ring.tailMap(hash);
            //Возвращает подкарту, содержащую все записи с ключом >= hash
            //Если такая подкарта не пуста, берём первый (наименьший) ключ из неё
            //Если подкарта пуста (хеш ключа больше всех хешей виртуальных узлов), значит возвращаемся к началу
            hash = tailMap.isEmpty() ? ring.firstKey() : tailMap.firstKey();
        }
        return ring.get(hash);
    }

    private long hash(String value) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(value.getBytes(StandardCharsets.UTF_8));
            long h = 0;
            for (int i = 0; i < 8; i++) {
                h <<= 8;
                h |= (digest[i] & 0xFF);
            }
            return h;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 algorithm not available", e);
        }
    }
}
