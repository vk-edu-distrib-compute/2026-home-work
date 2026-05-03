package company.vk.edu.distrib.compute.arseniy90;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.http.HttpClient;
import java.nio.file.Path;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.Map;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVCluster;

public class KVClusterImpl implements KVCluster {
    private static final String HOST_COLON = "http://localhost:";
    private static final String NODE_DATA_PATH_PREFIX = "node_";

    private final Map<String, SharedKVServiceImpl> nodes = new ConcurrentHashMap<>();
    private final List<String> endpoints;
    private final Path workingDir;
    private final HashStrategy hashStrategy;
    private final HttpClient sharedClient;

    public KVClusterImpl(List<Integer> ports, Path workingDir, HashStrategy hashStrategy) {
        this.endpoints = ports.stream()
                .sorted()
                .map(p -> HOST_COLON + p)
                .toList();
        this.workingDir = workingDir;
        this.hashStrategy = hashStrategy;
        this.sharedClient = HttpClient.newHttpClient();
    }

    @Override
    public void start() {
        endpoints.forEach(this::start);
    }

    @Override
    public void start(String endpoint) {
        nodes.computeIfAbsent(endpoint, e -> {
            try {
                Path nodePath = workingDir.resolve(NODE_DATA_PATH_PREFIX + e.hashCode());
                Dao<byte[]> dao = new FSDaoImpl(nodePath);
                HashRouter hashRouter = hashStrategy.createRouter(endpoints);
                SharedKVServiceImpl node = new SharedKVServiceImpl(e, hashRouter, dao, sharedClient);
                node.start();
                return node;
            } catch (IOException ex) {
                throw new UncheckedIOException("Failed to start node " + e, ex);
            }
        });
    }

    @Override
    public void stop() {
        nodes.values().forEach(SharedKVServiceImpl::stop);
        nodes.clear();
    }

    @Override
    public void stop(String endpoint) {
        SharedKVServiceImpl node = nodes.remove(endpoint);
        if (node != null) {
            node.stop();
        }
    }

    @Override
    public List<String> getEndpoints() {
         return new ArrayList<>(endpoints);
    }
}
