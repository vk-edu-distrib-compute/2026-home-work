package company.vk.edu.distrib.compute.aldor7705;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DorogovKVCluster implements KVCluster {
    private static final Logger log = LoggerFactory.getLogger(DorogovKVCluster.class);
    private final Map<Integer, KVService> services = new ConcurrentHashMap<>();
    private final Map<Integer, String> endpoints = new ConcurrentHashMap<>();

    public DorogovKVCluster(List<Integer> ports) {
        try {
            for (int port : ports) {
                KVServiceFactory kvServiceFactory = new KVServiceFactorySimple("storage_for_node_" + port, ports);
                services.put(port, kvServiceFactory.create(port));
                endpoints.put(port, "http://localhost:" + port);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void start() {
        for (KVService service : services.values()) {
            service.start();
        }
    }

    @Override
    public void start(String endpoint) {
        int port = extractPort(endpoint);
        KVService service = services.get(port);
        if (service != null) {
            log.info("Запуск узла на порту {}", port);
            service.start();
        }
    }

    @Override
    public void stop(String endpoint) {
        int port = extractPort(endpoint);
        KVService service = services.get(port);
        if (service != null) {
            log.info("Остановка узла на порту {}", port);
            service.stop();
        }
    }

    @Override
    public List<String> getEndpoints() {
        return new ArrayList<>(endpoints.values());
    }

    @Override
    public void stop() {
        log.info("Остановка всех узлов кластера");
        for (KVService service : services.values()) {
            service.stop();
        }
    }

    private int extractPort(String endpoint) {
        return Integer.parseInt(endpoint.split(":")[2].replace("/", ""));
    }
}
