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
            if (!Method.GET.name().equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, -1);
                return;
            }

            String path = exchange.getRequestURI().getPath();
            if (!path.startsWith(PREFIX)) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_NOT_FOUND, -1);
                return;
            }

            String tail = path.substring(PREFIX.length());
            if (tail.isEmpty()) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, -1);
                return;
            }

            boolean accessStats = false;
            if (tail.endsWith("/access")) {
                accessStats = true;
                tail = tail.substring(0, tail.length() - "/access".length());
            }

            int replicaId;
            try {
                replicaId = Integer.parseInt(tail);
            } catch (NumberFormatException e) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, -1);
                return;
            }

            Replica replica = replicas.getReplica(replicaId);
            if (replica == null) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_NOT_FOUND, -1);
                return;
            }

            String response;
            if (accessStats) {
                response = "reads=" + replica.readAccessCount()
                        + ",writes=" + replica.writeAccessCount() + "\n";
            } else {
                response = "keys=" + replica.keysCount() + "\n";
            }

            byte[] body = response.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        }
    }
}
