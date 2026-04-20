package company.vk.edu.distrib.compute.solntseva_nastya;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolntsevaKVService implements KVService {
    private static final Logger log = LoggerFactory.getLogger(SolntsevaKVService.class);

    private static final int STATUS_OK = 200;
    private static final int STATUS_CREATED = 201;
    private static final int STATUS_ACCEPTED = 202;
    private static final int STATUS_BAD_REQUEST = 400;
    private static final int STATUS_NOT_FOUND = 404;
    private static final int STATUS_NOT_ALLOWED = 405;
    private static final int NO_BODY = -1;

    private static final String ID_PARAM = "id";
    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";

    private final HttpServer server;
    private final Dao<byte[]> dao;
    private final String myUrl;
    private final HttpClient httpClient;
    
    // Одно поле для роутера вместо двух — Codacy будет доволен
    private final Router router;

    public SolntsevaKVService(final int port, final Dao<byte[]> dao,
                              final Set<String> topology, final String myUrl,
                              final SolnHashiStrategy strategy) throws IOException {
        this.dao = dao;
        this.myUrl = myUrl;

        // Инициализируем роутер сразу. Никаких null-assignment!
        if (strategy == SolnHashiStrategy.CONSISTENT) {
            this.router = new SolnConsistentHashRouter(topology);
        } else {
            this.router = new SolnRendezvousHashRouter(topology);
        }

        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .build();

        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/v0/status", this::handleStatus);
        server.createContext("/v0/entity", this::handleEntity);
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
        try {
            dao.close();
        } catch (final IOException e) {
            log.error("Error while closing DAO", e);
        }
    }

    private void handleStatus(final HttpExchange exchange) throws IOException {
        try (exchange) {
            if (METHOD_GET.equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(STATUS_OK, NO_BODY);
            } else {
                exchange.sendResponseHeaders(STATUS_NOT_ALLOWED, NO_BODY);
            }
        }
    }

    private void handleEntity(final HttpExchange exchange) {
        try (exchange) {
            final String query = exchange.getRequestURI().getQuery();
            final String id = extractId(query);

            if (id == null || id.isEmpty()) {
                exchange.sendResponseHeaders(STATUS_BAD_REQUEST, NO_BODY);
                return;
            }

            final String responsibleNode = router.getNode(id);
            if (!myUrl.equals(responsibleNode)) {
                proxyRequest(exchange, responsibleNode);
                return;
            }

            handleLocal(exchange, id);
        } catch (final IOException e) {
            log.error("Entity handling error", e);
        }
    }

    private void handleLocal(final HttpExchange exchange, final String id) throws IOException {
        final String method = exchange.getRequestMethod();
        switch (method) {
            case METHOD_GET -> handleGet(exchange, id);
            case METHOD_PUT -> handlePut(exchange, id);
            case METHOD_DELETE -> handleDelete(exchange, id);
            default -> exchange.sendResponseHeaders(STATUS_NOT_ALLOWED, NO_BODY);
        }
    }

    private void proxyRequest(final HttpExchange exchange, final String targetUrl) throws IOException {
        try {
            final URI uri = URI.create(targetUrl + exchange.getRequestURI().toString());
            final String method = exchange.getRequestMethod();
            
            final byte[] body = METHOD_PUT.equals(method) 
                ? exchange.getRequestBody().readAllBytes() 
                : new byte[0];
            
            final HttpRequest request = HttpRequest.newBuilder(uri)
                    .method(method, HttpRequest.BodyPublishers.ofByteArray(body))
                    .build();
            
            final HttpResponse<byte[]> resp = httpClient.send(request, 
                    HttpResponse.BodyHandlers.ofByteArray());
            
            final byte[] respBody = resp.body();
            exchange.sendResponseHeaders(resp.statusCode(), 
                    respBody.length == 0 ? NO_BODY : respBody.length);
            
            if (respBody.length > 0) {
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(respBody);
                }
            }
        } catch (final InterruptedException e) {
            log.error("Proxy request interrupted", e);
            Thread.currentThread().interrupt();
        }
    }

    private void handleGet(final HttpExchange exchange, final String id) throws IOException {
        try {
            final byte[] value = dao.get(id);
            exchange.sendResponseHeaders(STATUS_OK, value.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(value);
            }
        } catch (final NoSuchElementException e) {
            exchange.sendResponseHeaders(STATUS_NOT_FOUND, NO_BODY);
        }
    }

    private void handlePut(final HttpExchange exchange, final String id) throws IOException {
        dao.upsert(id, exchange.getRequestBody().readAllBytes());
        exchange.sendResponseHeaders(STATUS_CREATED, NO_BODY);
    }

    private void handleDelete(final HttpExchange exchange, final String id) throws IOException {
        dao.delete(id);
        exchange.sendResponseHeaders(STATUS_ACCEPTED, NO_BODY);
    }

    private static String extractId(final String query) {
        if (query == null) return null;
        for (final String param : query.split("&")) {
            final String[] kv = param.split("=", 2);
            if (kv.length == 2 && ID_PARAM.equals(kv[0])) {
                return kv[1];
            }
        }
        return null;
    }
}
