package company.vk.edu.distrib.compute.shuuuurik.routing;

import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import static company.vk.edu.distrib.compute.shuuuurik.util.HashUtils.hashToRing;

/**
 * Реализация Consistent Hashing.
 */
public class ConsistentHashRouter implements NodeRouter {

    /**
     * Выбирает ноду по принципу consistent hashing.
     * Строит кольцо из переданных нод и находит ближайшую по часовой стрелке.
     */
    @Override
    public String route(String key, List<String> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be null or empty");
        }

        // NavigableMap<позиция на кольце, endpoint ноды> - O(log n) поиск ближайшего элемента
        NavigableMap<Long, String> ring = buildRing(nodes);

        long keyHash = hashToRing(key);

        NavigableMap<Long, String> tailMap = ring.tailMap(keyHash, true);
        if (!tailMap.isEmpty()) {
            return tailMap.firstEntry().getValue();
        }

        // если ключ правее всех нод, идём к первой ноде кольца
        return ring.firstEntry().getValue();
    }

    /**
     * Строит кольцо: размещает каждую ноду по её хэшу.
     * Коллизии маловероятны при MD5, но TreeMap просто перезапишет одну из них.
     *
     * @param nodes список endpoint'ов
     * @return отсортированная карта позиций на кольце
     */
    private NavigableMap<Long, String> buildRing(List<String> nodes) {
        NavigableMap<Long, String> ring = new TreeMap<>();
        for (String node : nodes) {
            long position = hashToRing(node);
            ring.put(position, node);
        }
        return ring;
    }
}
