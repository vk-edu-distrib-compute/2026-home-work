package company.vk.edu.distrib.compute.shuuuurik.routing;

import java.util.List;

import static company.vk.edu.distrib.compute.shuuuurik.util.HashUtils.hashPair;

/**
 * Реализация Rendezvous Hashing (Highest Random Weight, HRW).
 */
public class RendezvousHashRouter implements NodeRouter {

    /**
     * Выбирает ноду с наибольшим хэшем пары (node, key).
     */
    @Override
    public String route(String key, List<String> nodes) {
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalArgumentException("nodes must not be null or empty");
        }

        String bestNode = null;
        long maxHash = Long.MIN_VALUE;

        for (String node : nodes) {
            long score = hashPair(node, key);
            if (score > maxHash) {
                maxHash = score;
                bestNode = node;
            }
        }

        return bestNode;
    }
}
