package company.vk.edu.distrib.compute.lillymega;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LillymegaKVCluster implements KVCluster {
    private final List<String> endpoints;
    private final Map<String, KVService> runningNodes = new ConcurrentHashMap<>();
    private final HttpClient httpClient = HttpClient.newHttpClient();

    public LillymegaKVCluster(List<Integer> ports) {
        List<Integer> ports1 = List.copyOf(ports);
        this.endpoints = ports1.stream()
                .map(port -> "http://localhost:" + port)
                .toList();
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
            return;
        }

        KVService service = createNode(endpoint);
        service.start();
        runningNodes.put(endpoint, service);
    }

    @Override
    public void stop() {
        for (String endpoint : List.copyOf(runningNodes.keySet())) {
            stop(endpoint);
        }
    }

    @Override
    public void stop(String endpoint) {
        KVService service = runningNodes.remove(endpoint);
        if (service != null) {
            service.stop();
        }
    }

    @Override
    public List<String> getEndpoints() {
        return endpoints;
    }

    private KVService createNode(String endpoint) {
        int port = extractPort(endpoint);
        Path dataFile = Path.of("data", "dao-" + port + ".data");
        try {
            return new LillymegaKVService(port, new PersistentDao(dataFile), endpoint, endpoints, httpClient);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create node for endpoint " + endpoint, e);
        }
    }

    private int extractPort(String endpoint) {
        int separatorIndex = endpoint.lastIndexOf(':');
        if (separatorIndex < 0 || separatorIndex == endpoint.length() - 1) {
            throw new IllegalArgumentException("Invalid endpoint: " + endpoint);
        }

        return Integer.parseInt(endpoint.substring(separatorIndex + 1));
    }
}
