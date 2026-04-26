package company.vk.edu.distrib.compute.expanse.core;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.expanse.utils.ExceptionUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class KVShardingClusterImpl implements KVCluster {
    private static Set<UUID> shardKeys;
    private static Map<UUID, String> shardKeyEndpointMap;
    private static Map<Integer, UUID> portShardKeyMap;
    private static Map<String, Node> endpointNodeMap;
    private static boolean isShardingEnabled;

    public KVShardingClusterImpl(int shardingFactor, List<Integer> ports) {
        if (ports.size() < shardingFactor) {
            throw new IllegalArgumentException("Not enough ports provided");
        }
        isShardingEnabled = true;
        shardKeyEndpointMap = new HashMap<>();
        portShardKeyMap = new HashMap<>();
        endpointNodeMap = new HashMap<>();

        ports.forEach(shardPort -> {
            UUID shardKey = UUID.randomUUID();
            String endpoint = "http://localhost:" + shardPort;

            shardKeyEndpointMap.put(shardKey, endpoint);
            portShardKeyMap.put(shardPort, shardKey);
            try {
                endpointNodeMap.put(endpoint, new Node(shardPort, shardPort + 1000));
            } catch (IOException e) {
                throw ExceptionUtils.wrapToInternal(e);
            }
        });
        shardKeys = Set.copyOf(shardKeyEndpointMap.keySet());
        shardKeyEndpointMap = Map.copyOf(shardKeyEndpointMap);
        portShardKeyMap = Map.copyOf(portShardKeyMap);
        endpointNodeMap = Map.copyOf(endpointNodeMap);
    }

    public static boolean isShardingEnabled() {
        return isShardingEnabled;
    }

    public static Set<UUID> getShardKeys() {
        return shardKeys;
    }

    public static Map<Integer, UUID> getPortShardKeyMap() {
        return portShardKeyMap;
    }

    public static Map<UUID, String> getShardKeyEndpointMap() {
        return shardKeyEndpointMap;
    }

    public static Map<String, Node> getEndpointNodeMap() {
        return endpointNodeMap;
    }

    @Override
    public void start() {
        endpointNodeMap.values().forEach(node -> {
            try {
                node.start();
            } catch (IOException ex) {
                throw ExceptionUtils.wrapToInternal(ex);
            }
        });
    }

    @Override
    public void start(String endpoint) {
        if (!endpointNodeMap.containsKey(endpoint)) {
            throw new IllegalArgumentException("Endpoint not found");
        }
        try {
            endpointNodeMap.get(endpoint).start();
        } catch (IOException e) {
            throw ExceptionUtils.wrapToInternal(e);
        }
    }

    @Override
    public void stop() {
        endpointNodeMap.values().forEach(Node::stop);
    }

    @Override
    public void stop(String endpoint) {
        if (!endpointNodeMap.containsKey(endpoint)) {
            throw new IllegalArgumentException("Endpoint not found");
        }
        endpointNodeMap.get(endpoint).stop();
    }

    @Override
    public List<String> getEndpoints() {
        return endpointNodeMap.keySet().stream().toList();
    }
}
