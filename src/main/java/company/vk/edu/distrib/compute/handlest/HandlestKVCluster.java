package company.vk.edu.distrib.compute.handlest;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.handlest.routing.HandlestRendezvousRouter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HandlestKVCluster implements KVCluster {

    private final List<String> endpoints;
    private final Map<String, HandlestService> nodeByEndpoint;

    public HandlestKVCluster(List<Integer> ports) throws IOException {
        endpoints = new ArrayList<>(ports.size());
        for (int port : ports) {
            endpoints.add("http://localhost:" + port);
        }

        HandlestRendezvousRouter router = new HandlestRendezvousRouter(endpoints);

        nodeByEndpoint = new HashMap<>(ports.size());
        for (int i = 0; i < ports.size(); i++) {
            registerNode(ports.get(i), endpoints.get(i), router);
        }
    }

    private void registerNode(int port, String endpoint, HandlestRendezvousRouter router) throws IOException {
        nodeByEndpoint.put(endpoint, new HandlestService(port, endpoint, router));
    }

    @Override
    public void start() {
        nodeByEndpoint.values().forEach(HandlestService::start);
    }

    @Override
    public void stop() {
        nodeByEndpoint.values().forEach(HandlestService::stop);
    }

    @Override
    public void start(String endpoint) {
        serviceFor(endpoint).start();
    }

    @Override
    public void stop(String endpoint) {
        serviceFor(endpoint).stop();
    }

    @Override
    public List<String> getEndpoints() {
        return Collections.unmodifiableList(endpoints);
    }

    private HandlestService serviceFor(String endpoint) {
        HandlestService service = nodeByEndpoint.get(endpoint);
        if (service == null) {
            throw new IllegalArgumentException("Unknown endpoint: " + endpoint);
        }
        return service;
    }
}
