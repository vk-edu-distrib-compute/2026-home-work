package company.vk.edu.distrib.compute.golubtsov_pavel;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PGCluster implements KVCluster {
    private static final Logger log = LoggerFactory.getLogger(PGCluster.class);
    private final List<Integer> ports;
    private final List<Integer> grpcPorts;
    private final List<String> endpoints;
    private final Map<String, KVService> runningNodes;
    private final Map<String, Integer> endpointToPort;
    private final Map<String, Integer> endpointToGrpcPort;

    public PGCluster(List<Integer> ports) {
        this.ports = List.copyOf(ports);
        this.grpcPorts = getGrpcPorts(this.ports);
        this.endpoints = java.util.stream.IntStream.range(0, this.ports.size())
                .mapToObj(index -> "http://localhost:" + this.ports.get(index)
                        + "?grpcPort=" + this.grpcPorts.get(index))
                .toList();
        this.runningNodes = new HashMap<>();
        this.endpointToPort = new HashMap<>();
        this.endpointToGrpcPort = new HashMap<>();

        for (int i = 0; i < this.ports.size(); i++) {
            endpointToPort.put(this.endpoints.get(i), this.ports.get(i));
            endpointToGrpcPort.put(this.endpoints.get(i), this.grpcPorts.get(i));
        }
    }

    private KVService createNode(int port, String endpoint) throws IOException {
        PGFileDao dao = new PGFileDao(Path.of("PGData", String.valueOf(port)));
        return new PGInMemoryKVService(port, endpointToGrpcPort.get(endpoint), dao, endpoint, endpoints);
    }

    private static List<Integer> getGrpcPorts(List<Integer> httpPorts) {
        try {
            List<Integer> result = new ArrayList<>();
            List<Integer> excludedPorts = new ArrayList<>(httpPorts);
            for (int ignored : httpPorts) {
                int grpcPort = PGgrpcKVService.Ports.availablePort(excludedPorts);
                result.add(grpcPort);
                excludedPorts.add(grpcPort);
            }
            return List.copyOf(result);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to reserve gRPC port", e);
        }
    }

    @Override
    public void start() {
        for (String endpoint : endpoints) {
            start(endpoint);
        }
    }

    @Override
    public void start(String endpoint) {
        if (runningNodes.containsKey(endpoint)) {
            log.info("node is already start");
            return;
        }
        Integer port = endpointToPort.get(endpoint);
        if (port == null) {
            throw new IllegalArgumentException("port is null");
        }
        try {
            KVService node = createNode(port, endpoint);
            node.start();
            runningNodes.put(endpoint, node);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start node " + endpoint, e);
        }
    }

    @Override
    public void stop() {
        for (String endpoint : endpoints) {
            stop(endpoint);
        }
    }

    @Override
    public void stop(String endpoint) {
        if (!runningNodes.containsKey(endpoint)) {
            log.info("node is not start");
            return;
        }
        KVService node = runningNodes.get(endpoint);
        node.stop();
        runningNodes.remove(endpoint);
    }

    @Override
    public List<String> getEndpoints() {
        return endpoints;
    }
}
