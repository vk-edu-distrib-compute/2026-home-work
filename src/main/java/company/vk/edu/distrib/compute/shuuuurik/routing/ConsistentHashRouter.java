package company.vk.edu.distrib.compute.shuuuurik.routing;

import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import static company.vk.edu.distrib.compute.shuuuurik.util.HashUtils.hashToRing;

/**
 * Реализация Consistent Hashing.
 */
public class ConsistentHashRouter implements NodeRouter {

    // NavigableMap<позиция на кольце, endpoint ноды> - O(log n) поиск ближайшего элемента
    private final NavigableMap<Long, String> ring;

    /**
     * Строит кольцо consistent hashing из переданных нод.
     *
     * @param nodes список endpoint'ов; не может быть null или пустым
     * @throws IllegalArgumentException если nodes null или пуст
     */
    public ConsistentHashRouter(List<String> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be null or empty");
        }
        this.ring = buildRing(nodes);
    }

    /**
     * Выбирает ноду по принципу consistent hashing: находит ближайшую по часовой стрелке.
     *
     * @param key   ключ запроса
     * @param nodes не используется (зафиксировано в конструкторе); не может быть null или пустым
     * @return endpoint выбранной ноды
     * @throws IllegalArgumentException если nodes null или пуст
     */
    @Override
    public String route(String key, List<String> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be null or empty");
        }
        // Параметр nodes не используется — кольцо построено в конструкторе.
        // Предполагается, что nodes совпадает со списком, переданным при создании.
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
