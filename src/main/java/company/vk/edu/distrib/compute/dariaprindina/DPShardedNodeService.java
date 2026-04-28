package company.vk.edu.distrib.compute.dariaprindina;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLEncoder;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.Objects;

public class DPShardedNodeService implements KVService {
    private static final Logger log = LoggerFactory.getLogger(DPShardedNodeService.class);
    private static final String ID_PARAM_PREFIX = "id=";
    private static final Duration PROXY_TIMEOUT = Duration.ofSeconds(2);

    private final String localEndpoint;
    private final HttpServer server;
    private final HttpClient httpClient;
    private final Dao<byte[]> dao;
    private final DPShardSelector shardSelector;

    public DPShardedNodeService(
        String localEndpoint,
        Dao<byte[]> dao,
        DPShardSelector shardSelector
    ) throws IOException {
        this.localEndpoint = Objects.requireNonNull(localEndpoint, "localEndpoint");
        this.dao = Objects.requireNonNull(dao, "dao");
        this.shardSelector = Objects.requireNonNull(shardSelector, "shardSelector");
        this.httpClient = HttpClient.newHttpClient();
        this.server = createHttpServer(localEndpoint);
        initServer();
    }

    private static HttpServer createHttpServer(String endpoint) throws IOException {
        final URI uri = URI.create(endpoint);
        return HttpServer.create(new InetSocketAddress(uri.getHost(), uri.getPort()), 0);
    }

    private void initServer() {
        server.createContext("/v0/status", new ErrorHttpHandler(exchange -> {
            if (Objects.equals("GET", exchange.getRequestMethod())) {
                sendResponse(exchange, 200, null);
            } else {
                sendResponse(exchange, 405, null);
            }
        }));

        server.createContext("/v0/entity", new ErrorHttpHandler(exchange -> {
            final String id = parseId(exchange.getRequestURI().getQuery());
            final String ownerEndpoint = shardSelector.ownerForKey(id);
            if (!localEndpoint.equals(ownerEndpoint)) {
                proxyToOwner(exchange, ownerEndpoint, id);
                return;
            }
            handleLocally(exchange, id);
        }));
    }

    private void handleLocally(HttpExchange exchange, String id) throws IOException {
        final String method = exchange.getRequestMethod();
        if ("GET".equals(method)) {
            final byte[] value = dao.get(id);
            sendResponse(exchange, 200, value);
            return;
        }
        if ("PUT".equals(method)) {
            try (var requestBody = exchange.getRequestBody()) {
                dao.upsert(id, requestBody.readAllBytes());
            }
            sendResponse(exchange, 201, null);
            return;
        }
        if ("DELETE".equals(method)) {
            dao.delete(id);
            sendResponse(exchange, 202, null);
            return;
        }
        sendResponse(exchange, 405, null);
    }

    private void proxyToOwner(HttpExchange exchange, String ownerEndpoint, String id) throws IOException {
        final String method = exchange.getRequestMethod();
        final String targetUrl = ownerEndpoint + "/v0/entity?id=" + URLEncoder.encode(id, StandardCharsets.UTF_8);
        final HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
            .uri(URI.create(targetUrl))
            .timeout(PROXY_TIMEOUT);

        if ("GET".equals(method)) {
            requestBuilder.GET();
        } else if ("DELETE".equals(method)) {
            requestBuilder.DELETE();
        } else if ("PUT".equals(method)) {
            final byte[] body;
            try (var requestBody = exchange.getRequestBody()) {
                body = requestBody.readAllBytes();
            }
            requestBuilder.PUT(HttpRequest.BodyPublishers.ofByteArray(body));
        } else {
            sendResponse(exchange, 405, null);
            return;
        }

        final HttpResponse<byte[]> response;
        try {
            response = httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofByteArray());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Proxy request interrupted", e);
        }
        sendResponse(exchange, response.statusCode(), response.body());
    }

    private static String parseId(String query) {
        if (query == null || !query.startsWith(ID_PARAM_PREFIX) || query.length() <= ID_PARAM_PREFIX.length()) {
            throw new IllegalArgumentException("bad query");
        }
        return URLDecoder.decode(query.substring(ID_PARAM_PREFIX.length()), StandardCharsets.UTF_8);
    }

    @Override
    public void start() {
        server.start();
        log.info("Node started. endpoint={}", localEndpoint);
    }

    @Override
    public void stop() {
        server.stop(0);
        log.info("Node stopped. endpoint={}", localEndpoint);
    }

    private static void sendResponse(HttpExchange exchange, int code, byte[] body) throws IOException {
        final byte[] responseBody = body == null ? new byte[0] : body;
        exchange.sendResponseHeaders(code, responseBody.length);
        if (responseBody.length > 0) {
            try (OutputStream outputStream = exchange.getResponseBody()) {
                outputStream.write(responseBody);
            }
            return;
        }
        exchange.getResponseBody().close();
    }

    private static final class ErrorHttpHandler implements HttpHandler {
        private final HttpHandler delegate;

        private ErrorHttpHandler(HttpHandler delegate) {
            this.delegate = delegate;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                delegate.handle(exchange);
            } catch (IllegalArgumentException e) {
                sendResponse(exchange, 400, null);
            } catch (NoSuchElementException e) {
                sendResponse(exchange, 404, null);
            } catch (IOException e) {
                sendResponse(exchange, 500, null);
            }
        }
    }
}
