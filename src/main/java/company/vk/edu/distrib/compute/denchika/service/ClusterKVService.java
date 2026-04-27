package company.vk.edu.distrib.compute.denchika.service;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.denchika.cluster.hashing.DistributingAlgorithm;
import company.vk.edu.distrib.compute.denchika.grpc.GrpcClusterClient;
import company.vk.edu.distrib.compute.denchika.grpc.GrpcClusterServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class ClusterKVService implements KVService {
    private static final Logger log = LoggerFactory.getLogger(ClusterKVService.class);

    private final int port;
    private final Dao<byte[]> dao;
    private final DistributingAlgorithm hasher;
    private final String myEndpoint;
    private final Map<String, Integer> grpcPorts;
    private final GrpcClusterServer grpcServer;
    private final GrpcClusterClient grpcClient;
    private HttpServer server;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public ClusterKVService(
        int port,
        int grpcPort,
        Dao<byte[]> dao,
        DistributingAlgorithm hasher,
        String myEndpoint,
        Map<String, Integer> grpcPorts
    ) {
        this.port = port;
        this.dao = dao;
        this.hasher = hasher;
        this.myEndpoint = myEndpoint;
        this.grpcPorts = grpcPorts;
        this.grpcServer = new GrpcClusterServer(grpcPort, dao);
        this.grpcClient = new GrpcClusterClient();
    }

    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            grpcServer.start();
            try {
                server = HttpServer.create(new InetSocketAddress(port), 0);
                server.createContext("/v0/status", this::handleStatus);
                server.createContext("/v0/entity", this::handleEntity);
                server.start();
                log.info("KVService started on port {}", port);
            } catch (IOException e) {
                running.set(false);
                grpcServer.stop();
                throw new IllegalStateException("Failed to start HTTP server on port " + port, e);
            }
        }
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            server.stop(0);
            grpcServer.stop();
            grpcClient.close();
            log.info("KVService stopped on port {}", port);
        }
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        try (exchange) {
            if ("GET".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(200, -1);
            } else {
                exchange.sendResponseHeaders(405, -1);
            }
        }
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        try (exchange) {
            String method = exchange.getRequestMethod();
            String query = exchange.getRequestURI().getQuery();
            if (query == null) {
                exchange.sendResponseHeaders(400, -1);
                return;
            }
            String id = parseParam(query);
            if (id == null || id.isBlank()) {
                exchange.sendResponseHeaders(400, -1);
                return;
            }
            String targetNode = hasher.selectNode(id);
            if (targetNode.equals(myEndpoint)) {
                handleLocally(exchange, method, id);
            } else {
                proxyViaGrpc(exchange, method, id, targetNode);
            }
        }
    }

    private void proxyViaGrpc(
        HttpExchange exchange,
        String method,
        String id,
        String targetEndpoint
    ) throws IOException {
        try {
            String host = URI.create(targetEndpoint).getHost();
            int grpcPort = grpcPorts.get(targetEndpoint);
            switch (method) {
                case "GET" -> {
                    byte[] data = grpcClient.get(host, grpcPort, id);
                    exchange.sendResponseHeaders(200, data.length);
                    exchange.getResponseBody().write(data);
                }
                case "PUT" -> {
                    byte[] data = exchange.getRequestBody().readAllBytes();
                    grpcClient.upsert(host, grpcPort, id, data);
                    exchange.sendResponseHeaders(201, -1);
                }
                case "DELETE" -> {
                    grpcClient.delete(host, grpcPort, id);
                    exchange.sendResponseHeaders(202, -1);
                }
                default -> exchange.sendResponseHeaders(405, -1);
            }
        } catch (NoSuchElementException e) {
            exchange.sendResponseHeaders(404, -1);
        } catch (Exception e) {
            log.error("gRPC proxy error for target {}", targetEndpoint, e);
            exchange.sendResponseHeaders(500, -1);
        }
    }

    private void handleLocally(HttpExchange exchange, String method, String id) throws IOException {
        switch (method) {
            case "GET" -> {
                try {
                    byte[] data = dao.get(id);
                    exchange.sendResponseHeaders(200, data.length);
                    exchange.getResponseBody().write(data);
                } catch (NoSuchElementException e) {
                    exchange.sendResponseHeaders(404, -1);
                }
            }
            case "PUT" -> {
                byte[] data = exchange.getRequestBody().readAllBytes();
                dao.upsert(id, data);
                exchange.sendResponseHeaders(201, -1);
            }
            case "DELETE" -> {
                dao.delete(id);
                exchange.sendResponseHeaders(202, -1);
            }
            default -> exchange.sendResponseHeaders(405, -1);
        }
    }

    private String parseParam(String query) {
        if (query == null) {
            return null;
        }
        Map<String, String> params = new ConcurrentHashMap<>();
        for (String pair : query.split("&")) {
            int eq = pair.indexOf('=');
            if (eq > 0) {
                params.put(pair.substring(0, eq), pair.substring(eq + 1));
            }
        }
        return params.get("id");
    }
}
