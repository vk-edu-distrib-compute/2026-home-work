package company.vk.edu.distrib.compute.vredakon;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class Strategies {
    private static SortedMap<Long, VredakonKVService> ring = new TreeMap<>();
    private static Map<Long, String> hashToString = new ConcurrentHashMap<>();
    private static final ReadWriteLock LOCK = new ReentrantReadWriteLock();
    private static final ShardingConfiguration CONFIG = new ShardingConfiguration();

    private Strategies() {
        throw new UnsupportedOperationException();
    }

    public static String resolve(String key, Map<String, VredakonKVService> nodes) {
        if ("rendezvous".equals(CONFIG.strategy())) {
            return resolveRendezvous(key, nodes);
        }          
        return resolveConsistent(key, nodes);
    }

    private static String resolveRendezvous(String key, Map<String, VredakonKVService> nodes) {
        String server = "";
        long maxHash = Long.MIN_VALUE;
        for (String node: nodes.keySet()) {
            long hash = HashUtils.hashSHA256(key + "#" + node);
            if (hash > maxHash) {
                maxHash = hash;
                server = node;
            }
        }
        return server;
    }

    private static String resolveConsistent(String key, Map<String, VredakonKVService> nodes) {
        if (ring.isEmpty()) {
            LOCK.writeLock().lock();
            for (var node: nodes.entrySet()) {
                long hash = HashUtils.hashSHA256(node.getKey());
                ring.put(hash, node.getValue());
                hashToString.put(hash, node.getKey());
            }
            LOCK.writeLock().unlock();
        }

        long keyHash = HashUtils.hashSHA256(key);

        SortedMap<Long, VredakonKVService> tail = ring.tailMap(keyHash);

        if (tail.isEmpty()) {
            return hashToString.get(ring.firstKey());
        }

        return hashToString.get(tail.firstKey());
    }

    public static void clear() {
        ring.clear();
        hashToString.clear();
    }
}

