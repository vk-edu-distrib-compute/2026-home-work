package company.vk.edu.distrib.compute.ce_fello;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.ReplicatedService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class CeFelloReplicatedKVService implements ReplicatedService {
    private static final Logger log = LoggerFactory.getLogger(CeFelloReplicatedKVService.class);
    private static final String REPLICA_DIRECTORY_PREFIX = "replica-";
    private static final String STATS_PATH = "/stats/replica";

    private final int port;
    private final int replicationFactor;
    private final HttpServer server;
    private final List<CeFelloReplicaNode> replicas;
    private final CeFelloReplicationCoordinator coordinator;
    private final ExecutorService executor;
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean stopped = new AtomicBoolean();
    private final CompletableFuture<Void> termination = new CompletableFuture<>();

    public CeFelloReplicatedKVService(int port, Path storageRoot, CeFelloReplicationConfig config) throws IOException {
        this.port = port;
        this.replicationFactor = config.replicationFactor();
        this.server = HttpServer.create();
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.replicas = createReplicas(Objects.requireNonNull(storageRoot, "storageRoot"), config.totalReplicas());
        this.coordinator = new CeFelloReplicationCoordinator(
                replicas,
                replicationFactor,
                new CeFelloReplicaPicker(config.totalReplicas()),
                executor,
                initialVersion(replicas)
        );

        server.setExecutor(executor);
        createContexts(replicationFactor);
    }

    @Override
    public synchronized void start() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("Service is already started");
        }

        try {
            bindAddress(port);
            server.start();
        } catch (IOException e) {
            started.set(false);
            throw new IllegalStateException("Failed to start service", e);
        }
    }

    @Override
    public synchronized void stop() {
        if (!started.get() || !stopped.compareAndSet(false, true)) {
            throw new IllegalStateException("Service is not running");
        }

        try {
            server.stop(0);
            executor.shutdownNow();
            for (CeFelloReplicaNode replica : replicas) {
                replica.close();
            }
        } finally {
            termination.complete(null);
        }
    }

    @Override
    public int port() {
        return port;
    }

    @Override
    public int numberOfReplicas() {
        return replicationFactor;
    }

    @Override
    public void disableReplica(int nodeId) {
        replica(nodeId).disable();
        log.info("Replica {} disabled", nodeId);
    }

    @Override
    public void enableReplica(int nodeId) {
        replica(nodeId).enable();
        log.info("Replica {} enabled", nodeId);
    }

    @Override
    public CompletableFuture<Void> awaitTermination() {
        return termination;
    }

    private void createContexts(int replicationFactor) {
        server.createContext(
                CeFelloClusterHttpHelper.STATUS_PATH,
                wrapHandler(new CeFelloClusterStatusHandler())
        );
        server.createContext(
                CeFelloClusterHttpHelper.ENTITY_PATH,
                wrapHandler(new CeFelloReplicatedEntityHandler(coordinator, replicationFactor))
        );
        server.createContext(STATS_PATH, wrapHandler(new CeFelloReplicaStatsHandler(replicas)));
    }

    private HttpHandler wrapHandler(HttpHandler handler) {
        return exchange -> {
            try (exchange) {
                try {
                    handler.handle(exchange);
                } catch (NoSuchElementException e) {
                    CeFelloClusterHttpHelper.sendEmpty(exchange, 404);
                } catch (IllegalArgumentException e) {
                    CeFelloClusterHttpHelper.sendEmpty(exchange, 400);
                } catch (CeFelloInsufficientAckException e) {
                    CeFelloClusterHttpHelper.sendEmpty(exchange, 500);
                } catch (Exception e) {
                    log.warn("Failed to handle request", e);
                    CeFelloClusterHttpHelper.sendEmpty(exchange, 503);
                }
            }
        };
    }

    @SuppressFBWarnings(
            value = "URLCONNECTION_SSRF_FD",
            justification = "The service binds only to localhost for homework tests"
    )
    private void bindAddress(int port) throws IOException {
        server.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 0);
    }

    private CeFelloReplicaNode replica(int nodeId) {
        if (nodeId < 0 || nodeId >= replicas.size()) {
            throw new IllegalArgumentException("Unknown replica id: " + nodeId);
        }
        return replicas.get(nodeId);
    }

    private static List<CeFelloReplicaNode> createReplicas(Path storageRoot, int totalReplicas) throws IOException {
        List<CeFelloReplicaNode> result = new ArrayList<>(totalReplicas);
        for (int i = 0; i < totalReplicas; i++) {
            result.add(new CeFelloReplicaNode(i, storageRoot.resolve(REPLICA_DIRECTORY_PREFIX + i)));
        }
        return List.copyOf(result);
    }

    private static long initialVersion(List<CeFelloReplicaNode> replicas) {
        long maxVersion = 0;
        for (CeFelloReplicaNode replica : replicas) {
            try {
                maxVersion = Math.max(maxVersion, replica.initializeMaxVersion());
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to scan replica " + replica.id(), e);
            }
        }
        return maxVersion;
    }
}
