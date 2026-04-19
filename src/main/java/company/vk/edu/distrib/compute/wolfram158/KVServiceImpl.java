package company.vk.edu.distrib.compute.wolfram158;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Executors;

public class KVServiceImpl implements KVService {
    private final HttpServer server;
    private Router router;
    private final HttpClient client;
    private final String endpoint;
    private static final String ENTITY_SUFFIX = "/v0/entity";

    public KVServiceImpl(final int port, final Dao<byte[]> dao) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        endpoint = Utils.mapToLocalhostEndpoint(port);
        client = HttpClient.newHttpClient();
        addStatusHandler();
        addEntityHandler(dao);
    }

    public void setRouter(Router router) {
        this.router = router;
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
    }

    private void addStatusHandler() {
        server.createContext("/v0/status", exchange -> {
            try (exchange) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, -1);
            } catch (IOException e) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_UNAVAILABLE, -1);
            }
        });
    }

    private void addEntityHandler(final Dao<byte[]> dao) {
        server.createContext(ENTITY_SUFFIX, exchange -> {
            if (router != null) {
                final Map<String, List<String>> queries = Utils.extractQueryParams(exchange.getRequestURI().getQuery());
                final List<String> values = queries.get(QueryParamConstants.ID);
                if (values.isEmpty() || values.getFirst().isEmpty()) {
                    exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, -1);
                    return;
                }
                try {
                    final String workEndpoint = router.getNode(values.getFirst());
                    if (!workEndpoint.equals(endpoint)) {
                        handleProxy(exchange, workEndpoint, values.getFirst());
                        return;
                    }
                } catch (NoSuchAlgorithmException ignored) {
                    return;
                }
            }
            switch (exchange.getRequestMethod()) {
                case HttpMethodConstants.GET: {
                    handleGetEntity(exchange, dao);
                    break;
                }

                case HttpMethodConstants.PUT: {
                    handlePutEntity(exchange, dao);
                    break;
                }

                case HttpMethodConstants.DELETE: {
                    handleDeleteEntity(exchange, dao);
                    break;
                }

                default: {
                    exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, -1);
                    exchange.close();
                    break;
                }
            }
        });
    }

    @SuppressWarnings("PMD.UseTryWithResources")
    private void handleProxy(
            final HttpExchange exchange,
            final String workEndpoint,
            final String key

    ) throws IOException {
        try {
            final String url = String.format(
                    "%s%s?%s=%s",
                    workEndpoint,
                    ENTITY_SUFFIX,
                    QueryParamConstants.ID,
                    URLEncoder.encode(key, StandardCharsets.UTF_8)
            );
            final HttpRequest.Builder builder = HttpRequest.newBuilder()
                    .uri(URI.create(url));
            switch (exchange.getRequestMethod()) {
                case HttpMethodConstants.GET -> builder.GET();
                case HttpMethodConstants.PUT ->
                        builder.PUT(HttpRequest.BodyPublishers.ofByteArray(exchange.getRequestBody().readAllBytes()));
                case HttpMethodConstants.DELETE -> builder.DELETE();
                default -> {
                    exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, -1);
                    return;
                }
            }
            final HttpResponse<byte[]> response = client.send(
                    builder.build(),
                    HttpResponse.BodyHandlers.ofByteArray()
            );
            exchange.sendResponseHeaders(response.statusCode(), response.body().length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.body());
            }
        } catch (Exception e) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_INTERNAL_ERROR, -1);
        } finally {
            exchange.close();
        }
    }

    @SuppressWarnings("PMD.UseTryWithResources")
    private void handleGetEntity(
            final HttpExchange exchange,
            final Dao<byte[]> dao
    ) throws IOException {
        try {
            final Map<String, List<String>> queries = Utils.extractQueryParams(exchange.getRequestURI().getQuery());
            final List<String> values = queries.get(QueryParamConstants.ID);
            if (values.isEmpty() || values.getFirst().isEmpty()) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, -1);
                return;
            }
            final byte[] response = dao.get(values.getFirst());
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, response.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response);
            }
        } catch (NoSuchElementException e) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_NOT_FOUND, -1);
        } finally {
            exchange.close();
        }
    }

    private void handlePutEntity(
            final HttpExchange exchange,
            final Dao<byte[]> dao
    ) throws IOException {
        try (exchange; InputStream is = exchange.getRequestBody()) {
            final Map<String, List<String>> queries = Utils.extractQueryParams(exchange.getRequestURI().getQuery());
            final List<String> values = queries.get(QueryParamConstants.ID);
            if (values.isEmpty() || values.getFirst().isEmpty()) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, -1);
                return;
            }
            dao.upsert(values.getFirst(), is.readAllBytes());
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_CREATED, -1);
        }
    }

    private void handleDeleteEntity(
            final HttpExchange exchange,
            final Dao<byte[]> dao
    ) throws IOException {
        try (exchange) {
            final Map<String, List<String>> queries = Utils.extractQueryParams(exchange.getRequestURI().getQuery());
            final List<String> values = queries.get(QueryParamConstants.ID);
            if (values.isEmpty() || values.getFirst().isEmpty()) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, -1);
            } else {
                dao.delete(values.getFirst());
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_ACCEPTED, -1);
            }
        }
    }
}
