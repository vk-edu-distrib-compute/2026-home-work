package company.vk.edu.distrib.compute.goshanchic;

import company.vk.edu.distrib.compute.KVService;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

public class KVServiceImpl implements KVService {
    private static final String PROXIED_HEADER = "X-Proxied";
    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";
    private static final int STATUS_OK = 200;
    private static final int STATUS_CREATED = 201;
    private static final int STATUS_ACCEPTED = 202;
    private static final int STATUS_BAD_REQUEST = 400;
    private static final int STATUS_NOT_FOUND = 404;
    private static final int STATUS_METHOD_NOT_ALLOWED = 405;
    private static final int STATUS_INTERNAL_ERROR = 500;
    private static final int STATUS_BAD_GATEWAY = 502;

    private final HttpServer server;
    private final InMemoryDao dao;
    private final List<String> clusterNodes;
    private final String selfAddress;
    private final HttpClient httpClient;
    private final RendezvousHasher hasher = new RendezvousHasher();

    public KVServiceImpl(int port, List<Integer> allPorts, InMemoryDao dao) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = dao;
        this.selfAddress = "http://localhost:" + port;
        this.clusterNodes = allPorts.stream()
                .map(p -> "http://localhost:" + p)
                .collect(Collectors.toList());
        this.httpClient = HttpClient.newHttpClient();
        setupEndpoints();
    }

    private void setupEndpoints() {
        server.createContext("/v0/status", exchange -> {
            try {
                sendResponse(exchange, STATUS_OK, "OK".getBytes());
            } catch (IOException e) {
                exchange.close();
            }
        });

        server.createContext("/v0/entity", exchange -> {
            try {
                String query = exchange.getRequestURI().getQuery();
                String id = extractId(query);
                if (id == null || id.isEmpty()) {
                    sendResponse(exchange, STATUS_BAD_REQUEST, "Bad Request".getBytes());
                    return;
                }

                String targetNode = hasher.getTargetNode(id, clusterNodes);

                if (targetNode.equals(selfAddress) || exchange.getRequestHeaders().containsKey(PROXIED_HEADER)) {
                    handleLocalRequest(exchange, id);
                } else {
                    proxyRequest(exchange, targetNode);
                }
            } catch (Exception e) {
                try {
                    sendResponse(exchange, STATUS_INTERNAL_ERROR, "Internal Server Error".getBytes());
                } catch (IOException ex) {
                    exchange.close();
                }
            }
        });
    }

    private void handleLocalRequest(HttpExchange exchange, String id) throws IOException {
        try {
            String method = exchange.getRequestMethod();
            if (METHOD_GET.equals(method)) {
                byte[] value = dao.get(id);
                sendResponse(exchange, STATUS_OK, value);
            } else if (METHOD_PUT.equals(method)) {
                byte[] body = exchange.getRequestBody().readAllBytes();
                dao.upsert(id, body);
                sendResponse(exchange, STATUS_CREATED, "Created".getBytes());
            } else if (METHOD_DELETE.equals(method)) {
                dao.delete(id);
                sendResponse(exchange, STATUS_ACCEPTED, "Accepted".getBytes());
            } else {
                sendResponse(exchange, STATUS_METHOD_NOT_ALLOWED, "Method Not Allowed".getBytes());
            }
        } catch (NoSuchElementException e) {
            sendResponse(exchange, STATUS_NOT_FOUND, "Not Found".getBytes());
        } catch (IllegalArgumentException e) {
            sendResponse(exchange, STATUS_BAD_REQUEST, "Bad Request".getBytes());
        }
    }

    private void proxyRequest(HttpExchange exchange, String targetNode) throws IOException {
        try {
            HttpRequest request = buildProxyRequest(exchange, targetNode);
            HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
            sendProxyResponse(exchange, response);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            sendResponse(exchange, STATUS_INTERNAL_ERROR, "Request interrupted".getBytes());
        } catch (Exception e) {
            sendResponse(exchange, STATUS_BAD_GATEWAY, "Bad Gateway".getBytes());
        }
    }

    private HttpRequest buildProxyRequest(HttpExchange exchange, String targetNode) throws IOException {
        URI uri = URI.create(targetNode + "/v0/entity?" + exchange.getRequestURI().getQuery());
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(uri)
                .header(PROXIED_HEADER, "true");

        String method = exchange.getRequestMethod();
        if (isMethodWithoutBody(method)) {
            requestBuilder.method(method, HttpRequest.BodyPublishers.noBody());
        } else if (METHOD_PUT.equals(method)) {
            byte[] body = exchange.getRequestBody().readAllBytes();
            requestBuilder.method(method, HttpRequest.BodyPublishers.ofByteArray(body));
        } else {
            throw new IllegalArgumentException("Unsupported method: " + method);
        }

        return requestBuilder.build();
    }

    private boolean isMethodWithoutBody(String method) {
        return METHOD_GET.equals(method) || METHOD_DELETE.equals(method);
    }

    private void sendProxyResponse(HttpExchange exchange, HttpResponse<byte[]> response) throws IOException {
        byte[] responseBody = response.body();
        exchange.sendResponseHeaders(response.statusCode(), responseBody != null ? responseBody.length : -1);
        if (responseBody != null && responseBody.length > 0) {
            exchange.getResponseBody().write(responseBody);
        }
        exchange.close();
    }

    private void sendResponse(HttpExchange exchange, int code, byte[] body) throws IOException {
        exchange.sendResponseHeaders(code, body != null ? body.length : -1);
        if (body != null && body.length > 0) {
            exchange.getResponseBody().write(body);
        }
        exchange.close();
    }

    private String extractId(String query) {
        if (query == null) {
            return null;
        }
        for (String param : query.split("&")) {
            String[] pair = param.split("=", 2);
            if (pair.length == 2 && "id".equals(pair[0])) {
                return pair[1];
            }
        }
        return null;
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
        } catch (IOException ex) {
            // Closing resources, exception can be safely ignored
        }
    }
}


