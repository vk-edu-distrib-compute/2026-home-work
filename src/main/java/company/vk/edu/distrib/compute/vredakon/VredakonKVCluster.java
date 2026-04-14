package company.vk.edu.distrib.compute.vredakon;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class VredakonKVCluster implements KVCluster {

    private final Map<String, KVService> nodes = new ConcurrentHashMap<>();
    private final KVServiceFactory factory = new VredakonKVServiceFactory();
    private final Logger log = LoggerFactory.getLogger(VredakonKVCluster.class);

    public VredakonKVCluster(List<Integer> ports) {
        if (ports.isEmpty()) {
            throw new IllegalStateException();
        }
        for (int port: ports) {
            try {
                VredakonKVService node = (VredakonKVService)factory.create(port);
                node.setOtherNodes(nodes);
                nodes.put("http://localhost:" + port, node);
            } catch (IOException exc) {
                log.error("Some error");
            }
        }
    }

    @Override
    public void start() {
        for (Map.Entry<String, KVService> node: nodes.entrySet()) {
            node.getValue().start();
        }
    }

    @Override
    public void start(String endpoint) {
        nodes.get(endpoint).start();
    }

    @Override
    public void stop() {
        for (Map.Entry<String, KVService> node: nodes.entrySet()) {
            node.getValue().stop();
        }
    }

    @Override
    public void stop(String endpoint) {
        nodes.get(endpoint).stop();
    }

    @Override
    public List<String> getEndpoints() {
        return List.copyOf(nodes.keySet());
    }
}
