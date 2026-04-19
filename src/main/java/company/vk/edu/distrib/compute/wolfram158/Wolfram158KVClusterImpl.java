package company.vk.edu.distrib.compute.wolfram158;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Wolfram158KVClusterImpl implements KVCluster {
    private static final Logger log = LoggerFactory.getLogger(Wolfram158KVClusterImpl.class);
    private final List<String> endpoints;
    private final Map<String, KVServiceImpl> endpointToKVService;
    private final KVServiceFactory factory;
    private final Router router;

    public Wolfram158KVClusterImpl(List<String> endpoints, KVServiceFactory factory) throws IOException,
            NoSuchAlgorithmException {
        this.endpoints = endpoints;
        this.endpointToKVService = new ConcurrentHashMap<>();
        this.factory = factory;
        this.router = new ConsistentHashing(endpoints, 100);
        for (String endpoint : endpoints) {
            final KVServiceImpl service = (KVServiceImpl) factory.create(Utils.extractPort(endpoint));
            service.setRouter(this.router);
            this.endpointToKVService.put(endpoint, service);
        }
    }

    @Override
    public void start() {
        endpointToKVService.forEach((endpoint, service) -> start(endpoint));
    }

    @Override
    public void start(String endpoint) {
        final KVService service = endpointToKVService.get(endpoint);
        if (service != null) {
            try {
                service.start();
            } catch (IllegalStateException ignored1) {
                createNewService(endpoint);
            }
        } else {
            createNewService(endpoint);
        }
    }

    private void createNewService(String endpoint) {
        try {
            final var newService = (KVServiceImpl) factory.create(Utils.extractPort(endpoint));
            newService.setRouter(router);
            newService.start();
            endpointToKVService.put(endpoint, newService);
        } catch (IOException e) {
            if (log.isErrorEnabled()) {
                log.error(e.getMessage());
            }
        }
    }

    @Override
    public void stop() {
        endpointToKVService.forEach((endpoint, service) -> stop(endpoint));
    }

    @Override
    public void stop(String endpoint) {
        final KVService service = endpointToKVService.get(endpoint);
        if (service != null) {
            service.stop();
        }
    }

    @Override
    public List<String> getEndpoints() {
        return endpoints;
    }
}
