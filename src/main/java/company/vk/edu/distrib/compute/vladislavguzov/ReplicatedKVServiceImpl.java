package company.vk.edu.distrib.compute.vladislavguzov;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.ReplicatedService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

public class ReplicatedKVServiceImpl implements ReplicatedService {

    private static final Logger log = LoggerFactory.getLogger(ReplicatedKVServiceImpl.class);
    private static final int NUM_REPLICAS = 3;

    private final int servicePort;
    private final MyKVCluster cluster;
    private final String[] replicaEndpoints;
    private final boolean[] enabled;
    private final HttpServer coordinatorServer;

    public ReplicatedKVServiceImpl(int port) throws IOException {
        this.servicePort = port;
        this.replicaEndpoints = new String[NUM_REPLICAS];
        this.enabled = new boolean[NUM_REPLICAS];

        List<Integer> replicaPorts = new ArrayList<>(NUM_REPLICAS);
        for (int i = 0; i < NUM_REPLICAS; i++) {
            int replicaPort = findFreePort();
            replicaPorts.add(replicaPort);
            replicaEndpoints[i] = "localhost:" + replicaPort;
            enabled[i] = true;
        }

        this.cluster = new MyKVCluster(replicaPorts);
        this.coordinatorServer = HttpServer.create(
                new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 0);
        installHandlers();
    }

    @Override
    public void start() {
        cluster.start();
        coordinatorServer.start();
        log.info("Replicated service started on port {}", servicePort);
    }

    @Override
    public void stop() {
        coordinatorServer.stop(0);
        for (int i = 0; i < NUM_REPLICAS; i++) {
            if (enabled[i]) {
                cluster.stop(replicaEndpoints[i]);
                enabled[i] = false;
            }
        }
        log.info("Replicated service stopped");
    }

    @Override
    public int port() {
        return servicePort;
    }

    @Override
    public int numberOfReplicas() {
        return NUM_REPLICAS;
    }

    @Override
    public void disableReplica(int nodeId) {
        if (!enabled[nodeId]) {
            return;
        }
        cluster.stop(replicaEndpoints[nodeId]);
        enabled[nodeId] = false;
        log.info("Replica {} disabled", nodeId);
    }

    @Override
    public void enableReplica(int nodeId) {
        if (enabled[nodeId]) {
            return;
        }
        cluster.start(replicaEndpoints[nodeId]);
        enabled[nodeId] = true;
        log.info("Replica {} enabled", nodeId);
    }

    private void installHandlers() {
        coordinatorServer.createContext("/v0/status", exchange -> {
            try (exchange) {
                int status = "GET".equals(exchange.getRequestMethod()) ? 200 : 405;
                exchange.sendResponseHeaders(status, -1);
            }
        });
        coordinatorServer.createContext("/v0/entity",
                new ReplicationHandler(cluster, replicaEndpoints, enabled));
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
