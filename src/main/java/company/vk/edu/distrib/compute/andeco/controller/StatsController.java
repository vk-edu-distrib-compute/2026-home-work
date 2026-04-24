package company.vk.edu.distrib.compute.andeco.controller;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.andeco.Method;
import company.vk.edu.distrib.compute.andeco.replica.Controller;
import company.vk.edu.distrib.compute.andeco.replica.Replica;
import company.vk.edu.distrib.compute.andeco.replica.Replicas;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;

import static company.vk.edu.distrib.compute.andeco.ServerConfigConstants.API_PATH;
import static company.vk.edu.distrib.compute.andeco.ServerConfigConstants.STATS_PATH;

public class StatsController implements Controller {
    private static final String PREFIX = API_PATH + STATS_PATH + "/replica/";

    private final Replicas replicas;

    public StatsController(Replicas replicas) {
        this.replicas = replicas;
    }

    @Override
    public void processRequest(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!isGet(exchange) || !hasValidPrefix(exchange)) {
                return;
            }

            ParsedPath parsedPath = parsePath(exchange);
            if (parsedPath == null) {
                return;
            }

            Replica replica = replicas.getReplica(parsedPath.replicaId());
            if (replica == null) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_NOT_FOUND, -1);
                return;
            }

            writeResponse(exchange, buildResponse(replica, parsedPath.accessStats()));
        }
    }

    private boolean isGet(HttpExchange exchange) throws IOException {
        if (!Method.GET.name().equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, -1);
            return false;
        }
        return true;
    }

    private boolean hasValidPrefix(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        if (!path.startsWith(PREFIX)) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_NOT_FOUND, -1);
            return false;
        }
        return true;
    }

    private ParsedPath parsePath(HttpExchange exchange) throws IOException {
        String tail = exchange.getRequestURI().getPath().substring(PREFIX.length());

        if (tail.isEmpty()) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, -1);
            return null;
        }

        boolean accessStats = false;
        if (tail.endsWith("/access")) {
            accessStats = true;
            tail = tail.substring(0, tail.length() - "/access".length());
        }

        try {
            return new ParsedPath(Integer.parseInt(tail), accessStats);
        } catch (NumberFormatException e) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, -1);
            return null;
        }
    }

    private String buildResponse(Replica replica, boolean accessStats) throws IOException {
        if (accessStats) {
            return "reads=" + replica.readAccessCount()
                    + ",writes=" + replica.writeAccessCount() + "\n";
        }
        return "keys=" + replica.keysCount() + "\n";
    }

    private void writeResponse(HttpExchange exchange, String response) throws IOException {
        byte[] body = response.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, body.length);

        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
    }

    private record ParsedPath(int replicaId, boolean accessStats) {
    }
}
