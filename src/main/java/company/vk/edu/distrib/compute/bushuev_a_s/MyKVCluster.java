package company.vk.edu.distrib.compute.bushuev_a_s;

import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.io.Serial;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MyKVCluster implements KVCluster {
    private final List<Integer> ports;
    private final MyHashingStrategy hashfunction;
    private final MyKVServiceFactory factory;
    private final List<KVService> activeServices = new ArrayList<>();
    private final List<Integer> activePorts = new ArrayList<>();
    private final Map<Integer, KVService> existingServices = new ConcurrentHashMap<>();

    public MyKVCluster(List<Integer> ports, MyHashingStrategy hashfunction) {
        this.ports = ports;
        this.hashfunction = hashfunction;
        factory = new MyKVServiceFactory(this);
    }

    public String getEndpoint(String id) throws NoAlgorithmException {
        final List<String> endpoints = new ArrayList<>();
        for (int port : activePorts) {
            endpoints.add("http://localhost:" + port);
        }
        try {
            return hashfunction.getEndpoint(id, endpoints);
        } catch (NoSuchAlgorithmException e) {
            throw new NoAlgorithmException("Failed to find hash algorithm", e);
        }
    }

    @Override
    public void start() {
        for (int port : ports) {
            start("http://localhost:" + port);
        }
    }

    @Override
    public void start(String endpoint) {
        final int portIndex = endpoint.lastIndexOf(':');
        final String portString = endpoint.substring(portIndex + 1);
        final int port = Integer.parseInt(portString);

        if (activePorts.contains(port)) {
            return;
        }

        final var existingService = existingServices.get(port);
        if (existingService != null) {
            existingService.start();
            activeServices.add(existingService);
            activePorts.add(port);
            return;
        }

        try {
            KVService service = factory.create(port);
            service.start();
            activeServices.add(service);
            activePorts.add(port);
        } catch (IOException e) {
            throw new StartException("Faild to start server", e);
        }
    }

    @Override
    public void stop() {
        for (KVService service : activeServices) {
            service.stop();
        }
        activeServices.clear();
        activePorts.clear();
    }

    @Override
    public void stop(String endpoint) {
        final int portIndex = endpoint.lastIndexOf(':');
        final String portString = endpoint.substring(portIndex + 1);
        final int port = Integer.parseInt(portString);

        for (int i = 0; i < activePorts.size(); i++) {
            if (activePorts.get(i) == port) {
                activeServices.get(i).stop();
                activeServices.remove(i);
                activePorts.remove(i);
                break;
            }
        }
    }

    @Override
    public List<String> getEndpoints() {
        final List<String> endpoints = new ArrayList<>();
        for (int port : ports) {
            endpoints.add("http://localhost:" + port);
        }
        return endpoints;
    }

    public class StartException extends RuntimeException {
        @Serial
        private static final long serialVersionUID = 1L;

        public StartException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    public class NoAlgorithmException extends RuntimeException {
        @Serial
        private static final long serialVersionUID = 1L;

        public NoAlgorithmException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
