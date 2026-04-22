package company.vk.edu.distrib.compute.glekoz.replica;

import company.vk.edu.distrib.compute.glekoz.KVServiceGK;
import company.vk.edu.distrib.compute.ReplicatedService;
import company.vk.edu.distrib.compute.glekoz.replica.records.ProxyResult;

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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.nio.ByteBuffer;

public class KVServiceReplicationGK extends KVServiceGK implements ReplicatedService {
    protected static final int INTERNAL_SERVER_ERROR = 500;
    private static final Logger log = LoggerFactory.getLogger(KVServiceReplicationGK.class);
    private final ReplicationManager repMan;
    private final HttpClient httpClient;
    private static final int MIN_ACKS = 1;

    public KVServiceReplicationGK(int port) {
        super(port);
        this.repMan = new ReplicationManager(port);
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    @Override
    protected void handleEntity(HttpExchange exchange) {
        try (exchange) {
            String id = extractId(exchange);
            if (id == null || id.isEmpty()) {
                exchange.sendResponseHeaders(STATUS_BAD_REQUEST, -1);
                return;
            }

            int ack = extractAck(exchange);
            if (ack > repMan.getReplicationFactor()) {
                exchange.sendResponseHeaders(STATUS_BAD_REQUEST, -1);
                return;
            }

            byte[] reqBody = exchange.getRequestBody().readAllBytes();
            
            List<ProxyResult> results = proxyToReplicas(exchange, id, reqBody); // new ArrayList<>();
            handleReplicasResponses(exchange, results, ack);

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
        return repMan.getReplicationFactor();
    }

    @Override
    public void disableReplica(int nodeId) {
        repMan.disableReplica(nodeId);
    }

    @Override
    public void enableReplica(int nodeId) {
        repMan.enableReplica(nodeId);
    }

    private List<ProxyResult> proxyToReplicas(HttpExchange exchange, String id, byte[] body) {
        List<ProxyResult> results = new ArrayList<>();
        for (String host : repMan.getReplicaHostList()) {
            ProxyResult res = proxyRequest(exchange, host, id, body);
            results.add(res);
        }
        return results;
    }

    private void handleReplicasResponses(HttpExchange exchange, List<ProxyResult> results, int ack) throws IOException {
        int successCount = 0;
        
        Map<Integer, Integer> statusCount = new ConcurrentHashMap<>();
        Map<ByteBuffer, Integer> bodyCount = new ConcurrentHashMap<>();
        
        for (ProxyResult r : results) {
            if (r.isSuccess()) {
                successCount++;
                
                int status = r.statusCode();
                statusCount.put(status, statusCount.getOrDefault(status, 0) + 1);
                
                ByteBuffer bodyKey = r.body() != null ? ByteBuffer.wrap(r.body()) : null;
                bodyCount.put(bodyKey, bodyCount.getOrDefault(bodyKey, 0) + 1);
            }
        }
        
        if (successCount < ack) {
            sendReplicaError(exchange, successCount, ack);
            return;
        }
        
        int mostFrequentStatus = findMostFrequentInt(statusCount);
        byte[] mostFrequentBody = findMostFrequentByteArray(bodyCount);
        
        sendSuccessResponse(exchange, mostFrequentStatus, mostFrequentBody);
    }

    private int findMostFrequentInt(Map<Integer, Integer> countMap) {
        int maxCount = 0;
        int mostFrequent = 400;
        
        for (Map.Entry<Integer, Integer> entry : countMap.entrySet()) {
            if (entry.getValue() > maxCount) {
                maxCount = entry.getValue();
                mostFrequent = entry.getKey();
            }
        }
        
        return mostFrequent;
    }

    private byte[] findMostFrequentByteArray(Map<ByteBuffer, Integer> countMap) {
        int maxCount = 0;
        ByteBuffer mostFrequent = null;
        
        for (Map.Entry<ByteBuffer, Integer> entry : countMap.entrySet()) {
            if (entry.getValue() > maxCount) {
                maxCount = entry.getValue();
                mostFrequent = entry.getKey();
            }
        }
        
        if (mostFrequent != null) {
            byte[] array = mostFrequent.array();
            byte[] result = new byte[array.length];
            System.arraycopy(array, mostFrequent.position(), result, 0, array.length);
            return result;
        }
        
        return new byte[0];
    }

    private void sendSuccessResponse(HttpExchange exchange, int status, byte[] body) throws IOException {
        String method = exchange.getRequestMethod();

        if (METHOD_GET.equals(method) && status != STATUS_NOT_FOUND) {
            exchange.getResponseHeaders().set(CONTENT_TYPE_HEADER, OCTET_STREAM);

            if (body == null || body.length == 0) {
                exchange.sendResponseHeaders(STATUS_OK, -1);
                return;
            }

            exchange.sendResponseHeaders(STATUS_OK, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
            return;
        }

        exchange.sendResponseHeaders(status, -1);
    }

    private void sendReplicaError(HttpExchange exchange, int ok, int ack) throws IOException {
        String msg = "Internal Server Error: only " + ok + "/" + ack + " replicas responded";
        byte[] bytes = msg.getBytes();

        exchange.sendResponseHeaders(500, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private ProxyResult proxyRequest(HttpExchange exchange, String targetNode, String id, byte[] body) {
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
            
            if (statusCode < INTERNAL_SERVER_ERROR) {
                return ProxyResult.success(statusCode, resBody);
            } else {
                return ProxyResult.error(statusCode);
            }
        } catch (Exception e) {
            log.error("Proxy request failed: {}", e);
            return ProxyResult.error(e);
        }
    }

    protected int extractAck(HttpExchange exchange) {
        int rf = repMan.getReplicationFactor();
        String query = exchange.getRequestURI().getQuery();
        if (query == null) {
            return rf / 2 + 1;
        }
        for (String param : query.split("&")) {
            if (param.startsWith("ack=")) {
                String value = param.substring(4);
                try {
                    int res = Integer.parseInt(value);
                    if (res < MIN_ACKS) {
                        log.warn("unacceptable ack value {}", res);
                        return rf / 2 + 1;
                    }
                    return res;
                } catch (NumberFormatException e) {
                    log.warn("unacceptable ack value {}", value, e);
                    return rf / 2 + 1;
                }
            }
        }
        return rf / 2 + 1;
    }
}
