package company.vk.edu.distrib.compute.maryarta;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;


import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KVClusterImpl implements KVCluster {
    private final List<Integer> ports;
    private final List<String> endpoints;// = getEndpoints();
    private final Map<String, KVService> kvServicesS = new HashMap<>();
//    private final Map<Integer, KVService> kvServices = new HashMap<>();

    public KVClusterImpl(List<Integer> ports) {
        this.ports = ports;
        this.endpoints = getEndpoints();
        for (Integer port: ports) {
            try {
                kvServicesS.put("http://localhost:" + port, new ShardedKVServiceImpl(port,"http://localhost:" + port, this.endpoints));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    // стартует все ноды кластера
    @Override
    public void start() {
//        List<String> endpoints = getEndpoints();
        for(String endpoint: endpoints){
            start(endpoint);
        }
    }

    // стартует одну определенную ноду кластера
    @Override
    public void start(String endpoint) {
        //проверить
//        URI uri = URI.create(endpoint);
//        int port = uri.getPort();
        try {
            kvServicesS.get(endpoint).start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        for(String endpoint: endpoints){
            stop(endpoint);
        }
    }

    @Override
    public void stop(String endpoint) {
        kvServicesS.get(endpoint).stop();
    }

    @Override
    public List<String> getEndpoints() {
        return ports.stream()
                .map(port -> "http://localhost:" + port).toList();
    }
}
