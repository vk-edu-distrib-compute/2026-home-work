package company.vk.edu.distrib.compute.borodinavalera1996dev.cluster;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.borodinavalera1996dev.*;
import company.vk.edu.distrib.compute.borodinavalera1996dev.hashing.ConsistentStrategy;
import company.vk.edu.distrib.compute.borodinavalera1996dev.hashing.HashingStrategy;
import company.vk.edu.distrib.compute.borodinavalera1996dev.hashing.RendezvousStrategy;
import org.h2.util.StringUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KVClusterFactoryImpl extends KVClusterFactory {

    public static final int NUMBER_OF_REPLICATION = 3;

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        String endpointPrefix = "http://localhost:";
        List<Node> nodes = new ArrayList<>();
        for (Integer integer : ports) {
            nodes.add(new Node(endpointPrefix + integer, true));
        }
        HashingStrategy strategy = getHashingStrategy(nodes);
        int numberOfReplications = getNumberOfReplications();

        KVProxyClient proxyClient = new KVProxyClient();
        Map<String, KVService> nodesMap = new ConcurrentHashMap<>();
        for (Integer port : ports) {
            String url = endpointPrefix + port;
            nodesMap.put(url, nodeService(port, url, strategy, proxyClient, numberOfReplications));
        }
        return new KVClusterImpl(nodesMap);
    }

    private static HashingStrategy getHashingStrategy(List<Node> nodes) {
        String hashingStrategy = System.getProperty("hashingStrategy");
        if (StringUtils.isNullOrEmpty(hashingStrategy) || "consistent".equals(hashingStrategy)) {
            return new ConsistentStrategy(nodes);
        } else {
            return new RendezvousStrategy(nodes);
        }
    }

    private static int getNumberOfReplications() {
        String numberOfReplications = System.getProperty("numberOfReplications");
        if (StringUtils.isNullOrEmpty(numberOfReplications)) {
            return NUMBER_OF_REPLICATION;
        } else {
            return Integer.parseInt(numberOfReplications);
        }
    }

    private static KVService nodeService(int port, String url, HashingStrategy strategy,
                                         KVProxyClient proxyClient, int numberOfReplications) {
        try {
            return new ClusteredKVServiceImpl(port, Path.of("storage","borodinavalera1996dev"),
                    strategy, url, proxyClient, numberOfReplications);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
