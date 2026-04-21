package company.vk.edu.distrib.compute.handlest;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.handlest.enums.HandlestHttpStatus;
import company.vk.edu.distrib.compute.handlest.exceptions.HttpClientException;
import company.vk.edu.distrib.compute.handlest.exceptions.PortRangeException;
import company.vk.edu.distrib.compute.handlest.exceptions.ServerStopException;
import company.vk.edu.distrib.compute.handlest.routing.HandlestRendezvousRouter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.NoSuchElementException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class HandlestService implements KVService {
    private final HttpServer server;
    private final HandlestDao dao;
    private final AtomicBoolean healthy = new AtomicBoolean(true);

    private final String selfEndpoint;
    private final HandlestRendezvousRouter router;
    private final HttpClient httpClient;

    private static final int MIN_PORT_VALUE = 1024;
    private static final int MAX_PORT_VALUE = 65535;
    private static final String ID_PARAM = "id";

    /** Standalone (non-cluster) constructor — backward-compatible. */
    public HandlestService(int port) throws IOException {
        this(port, null, null);
    }

    /**
     * Cluster-aware constructor.
     *
     * @param port         port this node listens on
     * @param selfEndpoint this node's own URL, e.g. "http://localhost:8080"
     * @param router       shared rendezvous router for key → node mapping
     */
    public HandlestService(int port, String selfEndpoint, HandlestRendezvousRouter router) throws IOException {
        if (port < MIN_PORT_VALUE || port > MAX_PORT_VALUE) {
            throw new PortRangeException(
                    "Port must be between " + MIN_PORT_VALUE + " and " + MAX_PORT_VALUE + ", got: " + port);
        }

        String storagePath = "./handlest_storage/handlest_cell_" + port;
        this.dao = new HandlestDao(storagePath);

        this.selfEndpoint = selfEndpoint;
        this.router = router;
        this.httpClient = HttpClient.newHttpClient();

        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/v0/status", this::handleStatus);
        server.createContext("/v0/entity", this::handleEntity);
        server.setExecutor(Executors.newFixedThreadPool(10));
    }

    @Override
    public void start() {
        server.start();
        healthy.set(true);
    }

    @Override
    public void stop() {
        healthy.set(false);
        server.stop(0);
        try {
            dao.close();
        } catch (IOException e) {
            throw new ServerStopException("Unexpected error occurred. Could not shutdown server", e);
        }
        try {
            httpClient.close();
        } catch (Exception e) {
            throw new HttpClientException("Unexpected error occurred. Could not stop httpClient", e);
        }
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        try (exchange) {
            if ("GET".equals(exchange.getRequestMethod())) {
                int statusCode = healthy.get()
                        ? HandlestHttpStatus.OK.getCode()
                        : HandlestHttpStatus.SERVICE_UNAVAILABLE.getCode();
                exchange.sendResponseHeaders(statusCode, -1);
            } else {
                exchange.sendResponseHeaders(HandlestHttpStatus.METHOD_NOT_ALLOWED.getCode(), -1);
            }
        }
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        try (exchange) {
            String method = exchange.getRequestMethod();
            String query = exchange.getRequestURI().getQuery();
            String id = extractId(query);

            if (id == null || id.isEmpty()) {
                exchange.sendResponseHeaders(HandlestHttpStatus.BAD_REQUEST.getCode(), -1);
                return;
            }

            if (tryProxy(exchange, id)) {
                return;
            }

            try {
                switch (method) {
                    case "GET" -> handleGet(exchange, id);
                    case "PUT" -> handlePut(exchange, id);
                    case "DELETE" -> handleDelete(exchange, id);
                    default -> exchange.sendResponseHeaders(
                            HandlestHttpStatus.METHOD_NOT_ALLOWED.getCode(), -1);
                }
            } catch (Exception e) {
                exchange.sendResponseHeaders(HandlestHttpStatus.INTERNAL_ERROR.getCode(), -1);
            }
        }
    }

    private void handleGet(HttpExchange exchange, String id) throws IOException {
        try {
            byte[] data = dao.get(id);
            exchange.sendResponseHeaders(HandlestHttpStatus.OK.getCode(), data.length);
            exchange.getResponseBody().write(data);
        } catch (NoSuchElementException e) {
            exchange.sendResponseHeaders(HandlestHttpStatus.NOT_FOUND.getCode(), -1);
        } catch (IllegalArgumentException e) {
            exchange.sendResponseHeaders(HandlestHttpStatus.BAD_REQUEST.getCode(), -1);
        }
    }

    private void handlePut(HttpExchange exchange, String id) throws IOException {
        try {
            byte[] data = exchange.getRequestBody().readAllBytes();
            dao.upsert(id, data);
            exchange.sendResponseHeaders(HandlestHttpStatus.CREATED.getCode(), -1);
        } catch (IllegalArgumentException e) {
            exchange.sendResponseHeaders(HandlestHttpStatus.BAD_REQUEST.getCode(), -1);
        }
    }

    private void handleDelete(HttpExchange exchange, String id) throws IOException {
        try {
            dao.delete(id);
            exchange.sendResponseHeaders(HandlestHttpStatus.ACCEPTED.getCode(), -1);
        } catch (IllegalArgumentException e) {
            exchange.sendResponseHeaders(HandlestHttpStatus.BAD_REQUEST.getCode(), -1);
        }
    }

    private void proxy(HttpExchange exchange, String targetEndpoint, String id) throws IOException {
        String url = targetEndpoint + "/v0/entity?id=" + id;
        String method = exchange.getRequestMethod();

        HttpRequest.Builder builder = HttpRequest.newBuilder().uri(URI.create(url));

        switch (method) {
            case "GET" -> builder.GET();
            case "PUT" -> {
                byte[] body = exchange.getRequestBody().readAllBytes();
                builder.PUT(HttpRequest.BodyPublishers.ofByteArray(body));
            }
            case "DELETE" -> builder.DELETE();
            default -> {
                exchange.sendResponseHeaders(HandlestHttpStatus.METHOD_NOT_ALLOWED.getCode(), -1);
                return;
            }
        }

        try {
            HttpResponse<byte[]> response =
                    httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());

            byte[] responseBody = response.body();
            if (responseBody != null && responseBody.length > 0) {
                exchange.sendResponseHeaders(response.statusCode(), responseBody.length);
                exchange.getResponseBody().write(responseBody);
            } else {
                exchange.sendResponseHeaders(
                        response.statusCode(),
                        responseBody != null ? 0 : -1);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            exchange.sendResponseHeaders(HandlestHttpStatus.INTERNAL_ERROR.getCode(), -1);
        }
    }

    private String extractId(String query) {
        if (query == null) {
            return null;
        }
        for (String param : query.split("&")) {
            String[] kv = param.split("=", 2);
            if (kv.length == 2 && ID_PARAM.equals(kv[0])) {
                return kv[1];
            }
        }
        return null;
    }

    private boolean tryProxy(HttpExchange exchange, String id) throws IOException {
        if (router == null) {
            return false;
        }
        String target = router.route(id);
        if (target.equals(selfEndpoint)) {
            return false;
        }
        proxy(exchange, target, id);
        return true;
    }
}
