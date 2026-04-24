package company.vk.edu.distrib.compute.tadzhnahal;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.ReplicatedService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class TadzhnahalKVService implements ReplicatedService {
    private static final String STATUS_PATH = "/v0/status";
    private static final String METHOD_GET = "GET";
    private static final String LOCALHOST = "http://localhost:";

    private final int port;
    private final Path rootDir;
    private final int replicaCount;
    private final List<TadzhnahalReplicaNode> replicaNodes;

    private final String localEndpoint;
    private final List<String> clusterEndpoints;
    private final TadzhnahalRendezvousHashing rendezvousHashing;
    private final TadzhnahalProxyClient proxyClient;

    private HttpServer server;
    private boolean started;

    public TadzhnahalKVService(int port, Path rootDir, int replicaCount) throws IOException {
        this(port, rootDir, replicaCount, List.of(buildEndpoint(port)));
    }

    public TadzhnahalKVService(
            int port,
            Path rootDir,
            int replicaCount,
            List<String> clusterEndpoints
    ) throws IOException {
        if (rootDir == null) {
            throw new IllegalArgumentException("Root dir must not be null");
        }

        if (replicaCount < 1) {
            throw new IllegalArgumentException("Replica count must be positive");
        }

        if (clusterEndpoints == null || clusterEndpoints.isEmpty()) {
            throw new IllegalArgumentException("Cluster endpoints must not be empty");
        }

        this.port = port;
        this.rootDir = rootDir;
        this.replicaCount = replicaCount;
        this.replicaNodes = createReplicaNodes(rootDir, replicaCount);

        this.localEndpoint = buildEndpoint(port);
        this.clusterEndpoints = prepareClusterEndpoints(clusterEndpoints, localEndpoint);
        this.rendezvousHashing = new TadzhnahalRendezvousHashing(this.clusterEndpoints);
        this.proxyClient = new TadzhnahalProxyClient();
    }

    @Override
    public void start() {
        if (started) {
            throw new IllegalStateException("Server already started");
        }

        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext(STATUS_PATH, this::handleStatus);
            server.createContext(
                    "/v0/entity",
                    new TadzhnahalEntityHandler(
                            localEndpoint,
                            replicaNodes.getFirst().dao(),
                            rendezvousHashing,
                            proxyClient
                    )
            );
            server.start();
            started = true;
        } catch (IOException e) {
            throw new IllegalStateException("Cannot start server", e);
        }
    }

    @Override
    public void stop() {
        if (!started) {
            throw new IllegalStateException("Server is not started");
        }

        server.stop(0);
        started = false;
        server = null;
    }

    @Override
    public int port() {
        return port;
    }

    @Override
    public int numberOfReplicas() {
        return replicaCount;
    }

    @Override
    public void disableReplica(int nodeId) {
        replica(nodeId).disable();
    }

    @Override
    public void enableReplica(int nodeId) {
        replica(nodeId).enable();
    }

    public Path rootDir() {
        return rootDir;
    }

    public List<TadzhnahalReplicaNode> replicaNodes() {
        return List.copyOf(replicaNodes);
    }

    private TadzhnahalReplicaNode replica(int nodeId) {
        if (nodeId < 0 || nodeId >= replicaNodes.size()) {
            throw new IllegalArgumentException("Unknown replica id: " + nodeId);
        }

        return replicaNodes.get(nodeId);
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!STATUS_PATH.equals(exchange.getRequestURI().getPath())) {
                sendEmptyResponse(exchange, 404);
                return;
            }

            if (!METHOD_GET.equals(exchange.getRequestMethod())) {
                sendEmptyResponse(exchange, 405);
                return;
            }

            sendEmptyResponse(exchange, 200);
        }
    }

    private void sendEmptyResponse(HttpExchange exchange, int code) throws IOException {
        exchange.sendResponseHeaders(code, -1);
    }

    private static String buildEndpoint(int port) {
        return LOCALHOST + port;
    }

    private static List<String> prepareClusterEndpoints(
            List<String> clusterEndpoints,
            String localEndpoint
    ) {
        List<String> endpoints = new ArrayList<>(clusterEndpoints);

        if (!endpoints.contains(localEndpoint)) {
            endpoints.add(localEndpoint);
        }

        return List.copyOf(endpoints);
    }

    private static List<TadzhnahalReplicaNode> createReplicaNodes(Path rootDir, int replicaCount)
            throws IOException {
        List<TadzhnahalReplicaNode> nodes = new ArrayList<>();

        for (int nodeId = 0; nodeId < replicaCount; nodeId++) {
            Path replicaDir = rootDir.resolve("replica-" + nodeId);
            FileDao dao = new FileDao(replicaDir);
            nodes.add(new TadzhnahalReplicaNode(nodeId, dao));
        }

        return List.copyOf(nodes);
    }
}
