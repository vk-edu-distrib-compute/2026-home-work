package company.vk.edu.distrib.compute.glekoz.replica;

import company.vk.edu.distrib.compute.glekoz.KVServiceGK;
import company.vk.edu.distrib.compute.ReplicatedService;

import com.sun.net.httpserver.HttpExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

record HostNode(String host, KVServiceGK node) {}

record ProxyResult(int statusCode, byte[] body, boolean isSuccess) {
    public static ProxyResult success(int statusCode, byte[] body) {
        return new ProxyResult(statusCode, body, true);
    }
    
    public static ProxyResult error(int statusCode) {
        return new ProxyResult(statusCode, null, false);
    }
    
    public static ProxyResult error(Exception e) {
        return new ProxyResult(500, null, false);
    }
}

public class KVServiceReplicationGK extends KVServiceGK implements ReplicatedService {
    protected static final Logger log = LoggerFactory.getLogger(KVServiceReplicationGK.class);
    private static final String LOCALHOST = "http://localhost:";
    private static final int MAX_PORT_ATTEMPTS = 5;

    private final Map<Integer, HostNode> nodesByIDs = new ConcurrentHashMap<>();
    private final int replicationFactor;

    private final HttpClient httpClient;

    public KVServiceReplicationGK(int port) {
        super(port);
        this.replicationFactor = getReplicationFactor();
        
        for (int nodeId = 0; nodeId < replicationFactor; nodeId++) {
            HostNode hostNode = startService(nodeId);
            nodesByIDs.put(nodeId, hostNode);
        }
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    @Override
    protected void handleEntity(HttpExchange exchange) {
        try (exchange) {
            if (!ENTITY_PATH.equals(exchange.getRequestURI().getPath())) {
                exchange.sendResponseHeaders(STATUS_NOT_FOUND, -1);
                return;
            }

            String id = extractId(exchange);
            if (id == null || id.isEmpty()) {
                exchange.sendResponseHeaders(STATUS_BAD_REQUEST, -1);
                return;
            }

            int ack = extractAck(exchange);

            if (ack > replicationFactor) {
                exchange.sendResponseHeaders(STATUS_BAD_REQUEST, -1);
                return;
            }

            byte[] reqBody = exchange.getRequestBody().readAllBytes();
            List<ProxyResult> list = new ArrayList<>();
            for (int nodeId = 0; nodeId < ack; nodeId++) {
                HostNode hostNode = nodesByIDs.get(nodeId);
                log.info("Proxying request for id={} to {}", id, hostNode.host());
                list.add(proxyRequest(exchange, hostNode.host(), id, reqBody));
            }
            int sc = 400;
            byte[] b = null;
            int okCount = 0;
            for (ProxyResult pr : list) {
                if (pr.isSuccess()) {
                    okCount++;
                    sc = pr.statusCode();
                    b = pr.body();
                }
            }
            String method = exchange.getRequestMethod();
            if (okCount >= ack) {
                if (METHOD_GET.equals(method) && sc != STATUS_NOT_FOUND) {
                    exchange.getResponseHeaders().set(CONTENT_TYPE_HEADER, OCTET_STREAM);
                    if (b != null) {
                        exchange.sendResponseHeaders(STATUS_OK, b.length == 0 ? -1 : b.length);
                    }
                     if (b != null && b.length > 0) {
                        try (OutputStream os = exchange.getResponseBody()) {
                            os.write(b);
                        }
                    }
                } else {
                    exchange.sendResponseHeaders(sc, -1);
                }
            } else {
                String errorMsg = "Internal Server Error: only " + okCount + "/" + ack + " replicas responded";
                exchange.sendResponseHeaders(500, errorMsg.getBytes().length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(errorMsg.getBytes());
                }
            }

        } catch (Exception e) {
            log.error("Internal error during request handling", e);
            sendSafeResponse(exchange, STATUS_BAD_REQUEST);
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
        HostNode hostNode = nodesByIDs.get(nodeId);
        if (hostNode == null) {
            log.warn("Attempted to disable non-existent replica with ID: {}", nodeId);
            return;
        }
        
        try {
            KVServiceGK service = hostNode.node();
            service.stop();
        } catch (Exception e) {
            log.error("Failed to disable replica with ID: {}", nodeId, e);
            throw new IllegalStateException("Failed to disable replica: " + nodeId, e);
        }
    }

    @Override
    public void enableReplica(int nodeId) {
        HostNode hostNode = nodesByIDs.get(nodeId);
        if (hostNode == null) {
            log.warn("Attempted to enable non-existent replica with ID: {}", nodeId);
            return;
        }
        
        try {
            hostNode.node().start(); 
        } catch (Exception e) {
            log.error("Failed to enable replica with ID: {}", nodeId, e);
            throw new IllegalStateException("Failed to enable replica: " + nodeId, e);
        }
    }

    private HostNode startService(int nodeId) {
        int portAttempt = 0;
        
        while (portAttempt < MAX_PORT_ATTEMPTS) {
            int nodePort = port + 1 + nodeId + portAttempt;
            
            try {
                KVServiceGK service = new KVServiceGK(nodePort);
                service.start();
                
                String endpoint = toEndpoint(nodePort);
                return new HostNode(endpoint, service);
                
            } catch (Exception e) {
                log.warn("Failed to start service on port {} for node ID {}: {}", 
                        port, nodeId, e.getMessage());
                portAttempt++;
                
                if (portAttempt >= MAX_PORT_ATTEMPTS) {
                    throw new IllegalStateException(
                        String.format("Failed to start service for node ID %d after %d attempts", 
                                    nodeId, MAX_PORT_ATTEMPTS), e);
                }
            }
        }
        
        throw new IllegalStateException("Unable to start service for node ID: " + nodeId);
    }

    private int getReplicationFactor() {
        String value = System.getenv("REPLICATION_FACTOR");

        if (value == null || value.isBlank()) {
            return 1;
        }

        try {
            int res = Integer.parseInt(value);
            if (res < 1) {
                throw new IllegalArgumentException("Replication factor must be > 0");
            }
            return res;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid replication factor env value: " + value, e);
        }
    }

    private String toEndpoint(int port) {
        return LOCALHOST + port;
    }

    private ProxyResult proxyRequest(HttpExchange exchange, String targetNode, String id, byte[] body) throws IOException {
        String method = exchange.getRequestMethod();
        String targetUrl = targetNode + ENTITY_PATH + "?" + ID_PARAM + "=" + id;
        
        try {
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                    .uri(URI.create(targetUrl))
                    .timeout(Duration.ofSeconds(10));
            
            switch (method) {
                case METHOD_GET -> requestBuilder.GET();
                case METHOD_PUT -> {
                    requestBuilder.PUT(HttpRequest.BodyPublishers.ofByteArray(body));
                    requestBuilder.header("Content-Type", "application/octet-stream");
                }
                case METHOD_DELETE -> requestBuilder.DELETE();
                default -> {
                    return ProxyResult.error(STATUS_METHOD_NOT_ALLOWED);
                }
            }
            
            HttpResponse<byte[]> response = httpClient.send(
                    requestBuilder.build(),
                    HttpResponse.BodyHandlers.ofByteArray()
            );

            int statusCode = response.statusCode();
            byte[] resBody = response.body();

            if (statusCode >= 200 && statusCode < 300 || statusCode == STATUS_NOT_FOUND) {
                return ProxyResult.success(statusCode, resBody);
            } else {
                return ProxyResult.error(statusCode);
            }
        } catch (Exception e) {
            log.error("Proxy request failed: {}", e.getMessage());
            return ProxyResult.error(e);
        }
    }

    protected int extractAck(HttpExchange exchange) {
        String query = exchange.getRequestURI().getQuery();
        if (query == null) {
            return replicationFactor / 2 + 1;
        }
        for (String param : query.split("&")) {
            if (param.startsWith("ack=")) {
                String value = param.substring(4);
                try {
                    int res = Integer.parseInt(value);
                    if (res < 1) {
                        log.warn("unacceptable ack value {}", res);
                        return replicationFactor / 2 + 1;
                    }
                    return res;
                } catch (NumberFormatException e) {
                    log.warn("unacceptable ack value {}", value, e);
                    return replicationFactor / 2 + 1;
                }
            }
        }
        return replicationFactor / 2 + 1;
    }
}