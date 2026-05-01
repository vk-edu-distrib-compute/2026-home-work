package company.vk.edu.distrib.compute.nesterukia.cluster;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.nesterukia.KVServiceImpl;
import company.vk.edu.distrib.compute.nesterukia.file_system.NesterukiaFileSystemKVServiceFactory;
import company.vk.edu.distrib.compute.nesterukia.utils.ClusterUtils;
import company.vk.edu.distrib.compute.nesterukia.utils.HashingAlgorithm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class NesterukiaKVCluster implements KVCluster {

    private static final Logger log = LoggerFactory.getLogger(NesterukiaKVCluster.class);
    private final Map<String, KVServiceImpl> nodesMap = new ConcurrentHashMap<>();
    private final KVServiceFactory kvServiceFactory;

    public NesterukiaKVCluster(List<Integer> ports, HashingAlgorithm algorithm) {

        Router routerImpl = Router.getRouterImpl(algorithm);
        this.kvServiceFactory = new NesterukiaFileSystemKVServiceFactory(
                routerImpl
        );

        ports.forEach(port -> {
            try {
                nodesMap.put(
                        ClusterUtils.portToEndpoint(port),
                        (KVServiceImpl) kvServiceFactory.create(port)
                );
            } catch (IOException e) {
                log.error("Error on cluster initialization: {}", e.getMessage());
            }
        });

        routerImpl.setNodesMap(nodesMap);
    }

    @Override
    public void start() {
        nodesMap.keySet().forEach(this::start);
    }

    @Override
    public void start(String endpoint) {
        if (nodesMap.containsKey(endpoint)) {
            nodesMap.get(endpoint).start();
        } else {
            log.warn("Error on service start. Service on endpoint='{}' does not exist.", endpoint);
        }
    }

    @Override
    public void stop() {
        nodesMap.keySet().forEach(this::stop);
    }

    @Override
    public void stop(String endpoint) {
        if (nodesMap.containsKey(endpoint)) {
            nodesMap.get(endpoint).stop();
        } else {
            log.warn("Error on service stop. Service on endpoint='{}' does not exist.", endpoint);
        }
    }

    @Override
    public List<String> getEndpoints() {
        return nodesMap.keySet().stream().toList();
    }
}
