package company.vk.edu.distrib.compute.artttnik;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.ReplicatedService;
import company.vk.edu.distrib.compute.artttnik.shard.RendezvousShardingStrategy;
import company.vk.edu.distrib.compute.artttnik.shard.ShardingStrategy;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MyReplicatedKVService implements ReplicatedService {

    private static final Logger log = LoggerFactory.getLogger(MyReplicatedKVService.class);

    private static final String ENTITY_PATH = "/v0/entity";
    private static final String GRPC_PORT_PARAM = "grpcPort";

    private static final int HTTP_OK = 200;
    private static final int DEFAULT_THREADS = 4;
    private static final int GRPC_PORT_OFFSET = 10_000;

    private final int servicePort;
    private final int grpcPort;
    private final MyReplicaManager replicaManager;
    private final ProxyService proxyService;
    private final LocalRequestHandler localHandler;

    private ExecutorService executor;
    private HttpServer server;
    private Server grpcServer;

    public MyReplicatedKVService(int port, List<Dao<byte[]>> replicaDaos) {
        this(port, defaultGrpcPort(port), replicaDaos, List.of(), new RendezvousShardingStrategy());
    }

    public MyReplicatedKVService(
            int port,
            List<Dao<byte[]>> replicaDaos,
            List<String> endpoints,
            ShardingStrategy shardingStrategy
    ) {
        this(port, defaultGrpcPort(port), replicaDaos, endpoints, shardingStrategy);
    }

    public MyReplicatedKVService(
            int port,
            int grpcPort,
            List<Dao<byte[]>> replicaDaos,
            List<String> endpoints,
            ShardingStrategy shardingStrategy
    ) {
        this.servicePort = port;
        this.grpcPort = grpcPort;
        this.replicaManager = new MyReplicaManager(replicaDaos);
        this.localHandler = new LocalRequestHandler(this.replicaManager);
        String selfEndpoint = formatEndpoint(port, grpcPort);
        this.proxyService = new ProxyService(selfEndpoint, endpoints, shardingStrategy);
    }

    public static int defaultGrpcPort(int httpPort) {
        return httpPort + GRPC_PORT_OFFSET;
    }

    public static String formatEndpoint(int httpPort, int grpcPort) {
        return "http://localhost:" + httpPort + "?" + GRPC_PORT_PARAM + "=" + grpcPort;
    }

    @Override
    public void start() {
        try {
            server = HttpServer.create(
                    new InetSocketAddress(InetAddress.getLoopbackAddress(), servicePort),
                    0
            );

            server.createContext("/v0/status", new StatusHandler());
            server.createContext(ENTITY_PATH, new EntityHandler(this));

            executor = Executors.newFixedThreadPool(DEFAULT_THREADS);
            server.setExecutor(executor);

            server.start();
                grpcServer = Grpc.newServerBuilderForPort(grpcPort, InsecureServerCredentials.create())
                    .addService(new InternalGrpcService(this))
                    .build()
                    .start();

            if (log.isInfoEnabled()) {
                log.info("KVService started on http={} grpc={}", servicePort, grpcPort);
            }

        } catch (IOException e) {
            log.error("Failed to start server on port {}", servicePort, e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void stop() {
        stopHttpServer();
        shutdownExecutor();
        shutdownGrpcServer();
        proxyService.close();
        closeReplicaManager();
        log.info("KVService stopped");
    }

    private void stopHttpServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    private void shutdownExecutor() {
        if (executor != null) {
            executor.shutdown();
        }
    }

    private void shutdownGrpcServer() {
        if (grpcServer != null) {
            grpcServer.shutdown();
            try {
                if (!grpcServer.awaitTermination(2, TimeUnit.SECONDS)) {
                    grpcServer.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                grpcServer.shutdownNow();
            }
        }
    }

    private void closeReplicaManager() {
        try {
            replicaManager.close();
        } catch (IOException e) {
            log.debug("Failed to close replica manager", e);
        }
    }

    @Override
    public int numberOfReplicas() {
        return replicaManager.numberOfReplicas();
    }

    @Override
    public int port() {
        return servicePort;
    }

    @Override
    public void disableReplica(int nodeId) {
        replicaManager.disableReplica(nodeId);
    }

    @Override
    public void enableReplica(int nodeId) {
        replicaManager.enableReplica(nodeId);
    }

    int parseAck(String ackValue) {
        return localHandler.parseAck(ackValue);
    }

    static Map<String, String> parseQueryParams(String query) {
        return LocalRequestHandler.parseQueryParams(query);
    }

    private static final class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            exchange.sendResponseHeaders(
                        "GET".equalsIgnoreCase(exchange.getRequestMethod()) ? HTTP_OK : 405,
                    -1
            );
        }
    }

    ProxyResult handleLocalRequest(String method, String id, int ack, byte[] body) {
        return localHandler.handleLocalRequest(method, id, ack, body);
    }

    ProxyService proxyService() {
        return proxyService;
    }
}
