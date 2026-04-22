package company.vk.edu.distrib.compute.glekoz.cluster;

import company.vk.edu.distrib.compute.glekoz.KVServiceGK;
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

public class KVClusteredServiceGK extends KVServiceGK {
    private static final Logger log = LoggerFactory.getLogger(KVClusteredServiceGK.class);
    private static final String LOCALHOST = "http://localhost:";

    private final ConsistentHash discovery;
    private final String host;
    private final HttpClient httpClient;

    public KVClusteredServiceGK(int port, ConsistentHash d) {
        super(port);
        this.discovery = d;
        this.host = LOCALHOST + port;
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

            String id = extractId(exchange.getRequestURI().getQuery());
            if (id == null || id.isEmpty()) {
                exchange.sendResponseHeaders(STATUS_BAD_REQUEST, -1);
                return;
            }

            String targetNode = discovery.getNode(id);
            if (!targetNode.equals(host)) {
                log.info("Proxying request for id={} from {} to {}", id, host, targetNode);
                proxyRequest(exchange, targetNode, id);
                return;
            }

            handleLocal(exchange, id);

        } catch (Exception e) {
            log.error("Internal error during request handling", e);
            sendSafeResponse(exchange, STATUS_BAD_REQUEST);
        }
    }

    private void handleLocal(HttpExchange exchange, String id) throws IOException {
        String method = exchange.getRequestMethod();
        switch (method) {
            case METHOD_GET -> handleGet(exchange, id);
            case METHOD_PUT -> handlePut(exchange, id);
            case METHOD_DELETE -> handleDelete(exchange, id);
            default -> exchange.sendResponseHeaders(STATUS_METHOD_NOT_ALLOWED, -1);
        }
    }

    private void proxyRequest(HttpExchange exchange, String targetNode, String id) throws IOException {
        String method = exchange.getRequestMethod();
        String targetUrl = targetNode + ENTITY_PATH + "?" + ID_PARAM + "=" + id;
        
        try {
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                    .uri(URI.create(targetUrl))
                    .timeout(Duration.ofSeconds(10));
            
            switch (method) {
                case METHOD_GET -> requestBuilder.GET();
                case METHOD_PUT -> {
                    byte[] body = exchange.getRequestBody().readAllBytes();
                    requestBuilder.PUT(HttpRequest.BodyPublishers.ofByteArray(body));
                }
                case METHOD_DELETE -> requestBuilder.DELETE();
                default -> {
                    exchange.sendResponseHeaders(STATUS_METHOD_NOT_ALLOWED, -1);
                    return;
                }
            }
            
            HttpResponse<byte[]> response = httpClient.send(
                    requestBuilder.build(),
                    HttpResponse.BodyHandlers.ofByteArray()
            );
            
            exchange.getResponseHeaders().putAll(response.headers().map());
            exchange.sendResponseHeaders(response.statusCode(), response.body().length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.body());
            }
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Proxy request interrupted", e);
            exchange.sendResponseHeaders(STATUS_BAD_REQUEST, -1);
        } catch (Exception e) {
            log.error("Failed to proxy request to {}", targetNode, e);
            exchange.sendResponseHeaders(STATUS_BAD_REQUEST, -1);
        }
    }
}
