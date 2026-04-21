package company.vk.edu.distrib.compute.tadzhnahal;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class TadzhnahalKVService implements KVService {
    private static final String STATUS_PATH = "/v0/status";
    private static final String ENTITY_PATH = "/v0/entity";
    private static final String METHOD_GET = "GET";
    private static final String LOCALHOST = "http://localhost:";

    private final int port;
    private final Dao<byte[]> dao;
    private final String localEndpoint;
    private final List<String> clusterEndpoints;
    private final TadzhnahalShardingAlgorithm shardingAlgorithm;
    private final TadzhnahalShardSelector shardSelector;
    private final TadzhnahalProxyClient proxyClient;

    private HttpServer server;
    private boolean started;

    public TadzhnahalKVService(int port, Dao<byte[]> dao) {
        this(
                port,
                dao,
                List.of(buildEndpoint(port)),
                TadzhnahalShardingAlgorithm.RENDEZVOUS
        );
    }

    public TadzhnahalKVService(int port, Dao<byte[]> dao, List<String> clusterEndpoints) {
        this(
                port,
                dao,
                clusterEndpoints,
                TadzhnahalShardingAlgorithm.RENDEZVOUS
        );
    }

    public TadzhnahalKVService(
            int port,
            Dao<byte[]> dao,
            List<String> clusterEndpoints,
            TadzhnahalShardingAlgorithm shardingAlgorithm
    ) {
        if (dao == null) {
            throw new IllegalArgumentException("Dao must not be null");
        }

        if (clusterEndpoints == null || clusterEndpoints.isEmpty()) {
            throw new IllegalArgumentException("Cluster endpoints must not be empty");
        }

        if (shardingAlgorithm == null) {
            throw new IllegalArgumentException("Sharding algorithm must not be null");
        }

        this.port = port;
        this.dao = dao;
        this.localEndpoint = buildEndpoint(port);
        this.clusterEndpoints = prepareClusterEndpoints(clusterEndpoints, localEndpoint);
        this.shardingAlgorithm = shardingAlgorithm;
        this.shardSelector = createShardSelector(this.clusterEndpoints, this.shardingAlgorithm);
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
                    ENTITY_PATH,
                    new TadzhnahalEntityHandler(
                            localEndpoint,
                            dao,
                            shardSelector,
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
        Set<String> uniqueEndpoints = new LinkedHashSet<>(clusterEndpoints);
        uniqueEndpoints.add(localEndpoint);
        return new ArrayList<>(uniqueEndpoints);
    }

    private static TadzhnahalShardSelector createShardSelector(
            List<String> clusterEndpoints,
            TadzhnahalShardingAlgorithm shardingAlgorithm
    ) {
        if (TadzhnahalShardingAlgorithm.CONSISTENT == shardingAlgorithm) {
            return new TadzhnahalConsistentHashing(clusterEndpoints);
        }

        return new TadzhnahalRendezvousHashing(clusterEndpoints);
    }
}
