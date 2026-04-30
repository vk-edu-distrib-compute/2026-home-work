package company.vk.edu.distrib.compute.nihuaway00.bootstrap;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVClusterFactory;
import company.vk.edu.distrib.compute.nihuaway00.Config;
import company.vk.edu.distrib.compute.nihuaway00.Cluster;
import company.vk.edu.distrib.compute.nihuaway00.cluster.*;
import company.vk.edu.distrib.compute.nihuaway00.transport.grpc.GrpcChannelRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterFactory extends KVClusterFactory {
    private static final Logger log = LoggerFactory.getLogger(ClusterFactory.class);

    @Override
    protected KVCluster doCreate(List<Integer> ports) {
        Map<String, ClusterNode> nodes = new ConcurrentHashMap<>();
        Map<Integer, Integer> grpcPortByHttpPort = allocateGrpcPorts(ports);

        ports.forEach(port -> {
            String endpoint = "http://localhost:" + (port);
            String grpcEndpoint = "localhost:" + grpcPortByHttpPort.get(port);
            nodes.put(endpoint, new ClusterNode(endpoint, grpcEndpoint));
        });

        GrpcChannelRegistry channelRegistry = new GrpcChannelRegistry();

        String shardingStrategyProp = Config.strategy();
        if (log.isInfoEnabled()) {
            log.info("Using strategy: {}", shardingStrategyProp);
        }
        ShardingStrategy strategy = "rendezvous".equals(shardingStrategyProp)
                ? new RendezvousHashingStrategy(nodes)
                : new ConsistentHashingStrategy(nodes, 50);

        int replicaCountProps = Config.replicas();

        ServiceFactory serviceFactory = new ServiceFactory(
                strategy,
                replicaCountProps,
                channelRegistry,
                grpcPortByHttpPort
        );

        return new Cluster(strategy, serviceFactory, channelRegistry);
    }

    private Map<Integer, Integer> allocateGrpcPorts(List<Integer> httpPorts) {
        Map<Integer, Integer> grpcPorts = new ConcurrentHashMap<>();
        Set<Integer> reservedPorts = new HashSet<>(httpPorts);

        for (Integer httpPort : httpPorts) {
            int grpcPort = findAvailablePort(httpPort + 1, reservedPorts);
            reservedPorts.add(grpcPort);
            grpcPorts.put(httpPort, grpcPort);
        }

        return grpcPorts;
    }

    private int findAvailablePort(int preferredPort, Set<Integer> reservedPorts) {
        int first = Math.max(1024, preferredPort);
        for (int port = first; port < 65535; port++) {
            if (!reservedPorts.contains(port) && isTcpPortAvailable(port)) {
                return port;
            }
        }

        for (int port = 1024; port < first; port++) {
            if (!reservedPorts.contains(port) && isTcpPortAvailable(port)) {
                return port;
            }
        }

        throw new IllegalStateException("Can't allocate gRPC port");
    }

    private boolean isTcpPortAvailable(int port) {
        try (ServerSocket serverSocket = new ServerSocket()) {
            serverSocket.setReuseAddress(false);
            serverSocket.bind(new InetSocketAddress(InetAddress.getByName("localhost"), port), 1);
            return true;
        } catch (IOException ex) {
            return false;
        }
    }
}
