package company.vk.edu.distrib.compute.linempy.scharding;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.linempy.KVServiceImpl;
import company.vk.edu.distrib.compute.linempy.routing.ShardingStrategy;
import company.vk.edu.distrib.compute.linempy.scharding.proxy.ProxyClient;
import company.vk.edu.distrib.compute.linempy.scharding.proxy.ProxyResponse;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class KVServiceProxyImpl extends KVServiceImpl {
    private static final Logger log = LoggerFactory.getLogger(KVServiceProxyImpl.class);

    private final ProxyClient proxyClient;
    private final ShardingStrategy shardingStrategy;
    private final List<String> allEndpoints;
    private final String selfEndpoint;
    private final Server grpcServer;

    public KVServiceProxyImpl(int port, Dao<byte[]> dao,
                              List<String> allEndpoints,
                              ShardingStrategy shardingStrategy,
                              ProxyClient proxyClient) throws IOException {
        super(dao, port);
        this.selfEndpoint = "http://localhost:" + port;
        this.allEndpoints = allEndpoints;
        this.shardingStrategy = shardingStrategy;
        this.proxyClient = proxyClient;

        int grpcPort = port + 100;
        this.grpcServer = ServerBuilder.forPort(grpcPort)
                .addService(new LinempyGrpcService(dao))
                .build();
    }

    @Override
    protected void entityHandler(HttpExchange exchange) throws IOException {
        String id = detachedId(exchange.getRequestURI().getQuery());
        if (id == null || id.isEmpty()) {
            exchange.sendResponseHeaders(400, -1);
            return;
        }

        String targetNode = shardingStrategy.route(id, allEndpoints);
        String targetBase = targetNode.split("\\?")[0];

        if (selfEndpoint.equals(targetBase)) {
            super.entityHandler(exchange);
        } else {
            proxyRequest(exchange, id, targetNode);
        }
    }

    private void proxyRequest(HttpExchange exchange, String id, String targetNode) throws IOException {
        String targetUrl = targetNode.split("\\?")[0];
        String method = exchange.getRequestMethod();
        CompletableFuture<ProxyResponse> future;

        try {
            switch (method) {
                case "GET" -> future = proxyClient.get(targetUrl, id);
                case "PUT" -> {
                    byte[] body = exchange.getRequestBody().readAllBytes();
                    future = proxyClient.put(targetUrl, id, body);
                }
                case "DELETE" -> future = proxyClient.delete(targetUrl, id);
                default -> {
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }
            }

            ProxyResponse response = future.join();
            exchange.sendResponseHeaders(response.statusCode(), response.body().length);
            if (response.body().length > 0) {
                exchange.getResponseBody().write(response.body());
            }
        } catch (Exception e) {
            log.error("Proxy request failed", e);
            exchange.sendResponseHeaders(500, -1);
        }
    }

    @Override
    public void start() {
        super.start();
        try {
            grpcServer.start();
            log.info("gRPC server started on port {}", grpcServer.getPort());
        } catch (IOException e) {
            log.error("Failed to start gRPC server", e);
        }
    }

    @Override
    public void stop() {
        grpcServer.shutdown();
        if (proxyClient != null) {
            proxyClient.close();
        }
        super.stop();
    }
}
