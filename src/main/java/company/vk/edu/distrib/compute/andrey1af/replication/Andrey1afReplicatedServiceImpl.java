package company.vk.edu.distrib.compute.andrey1af.replication;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.ReplicatedService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class Andrey1afReplicatedServiceImpl implements ReplicatedService {
    private static final int DEFAULT_REPLICA_COUNT = 3;
    private static final String REPLICA_COUNT_PROPERTY = "andrey1af.replication.replicas";
    private static final String REPLICA_COUNT_ENV = "ANDREY1AF_REPLICATION_REPLICAS";
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String STATUS_PATH = "/v0/status";
    private static final int HTTP_OK = 200;

    private final int servicePort;
    private final HttpServer server;
    private final ExecutorService executor;
    private final ReplicatedStorage storage;
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    public Andrey1afReplicatedServiceImpl(int port) throws IOException {
        this(port, configuredReplicaCount());
    }

    public Andrey1afReplicatedServiceImpl(int port, int replicaCount) throws IOException {
        this.servicePort = port;
        this.storage = new ReplicatedStorage(replicaCount);
        this.executor = Executors.newFixedThreadPool(Math.max(2, Runtime.getRuntime().availableProcessors()));
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.server.setExecutor(executor);
        initServer();
    }

    @Override
    public int port() {
        return servicePort;
    }

    @Override
    public int numberOfReplicas() {
        return storage.numberOfReplicas();
    }

    @Override
    public void disableReplica(int nodeId) {
        storage.disableReplica(nodeId);
    }

    @Override
    public void enableReplica(int nodeId) {
        storage.enableReplica(nodeId);
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }

        server.stop(1);
        executor.shutdownNow();
    }

    private void initServer() {
        server.createContext(STATUS_PATH, exchange -> {
            exchange.sendResponseHeaders(HTTP_OK, -1);
            exchange.close();
        });
        server.createContext(ENTITY_PATH, new ReplicatedEntityHandler(storage));
    }

    private static int configuredReplicaCount() {
        String configuredValue = System.getProperty(REPLICA_COUNT_PROPERTY);
        if (configuredValue == null || configuredValue.isBlank()) {
            configuredValue = System.getenv(REPLICA_COUNT_ENV);
        }
        if (configuredValue == null || configuredValue.isBlank()) {
            return DEFAULT_REPLICA_COUNT;
        }

        int replicaCount;
        try {
            replicaCount = Integer.parseInt(configuredValue);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid replica count: " + configuredValue, e);
        }
        if (replicaCount < DEFAULT_REPLICA_COUNT) {
            throw new IllegalArgumentException("Replica count must be at least " + DEFAULT_REPLICA_COUNT);
        }
        return replicaCount;
    }
}
