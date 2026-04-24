package company.vk.edu.distrib.compute.andeco.replica;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.andeco.Method;
import company.vk.edu.distrib.compute.andeco.QueryUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;

public class ReplicaEntityController implements Controller {
    private final Replicas replicas;

    public ReplicaEntityController(int port, int n) {
        this.replicas = new Replicas(port, n);
    }

    @Override
    public void processRequest(HttpExchange exchange) throws IOException {
        try (exchange) {
            String id = QueryUtil.extractId(exchange.getRequestURI().getQuery());
            Integer ack = QueryUtil.extractAck(exchange.getRequestURI().getQuery());

            int effectiveAck = ack == null ? 1 : ack;

            if (effectiveAck <= 0 || effectiveAck > replicas.getNumberOfReplicas()) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, -1);
                return;
            }

            Method method = Method.valueOf(exchange.getRequestMethod());

            switch (method) {
                case GET -> processGet(exchange, id, effectiveAck);
                case PUT -> processPut(exchange, id, effectiveAck);
                case DELETE -> processDelete(exchange, id, effectiveAck);
            }
        }
    }

    private void processGet(HttpExchange exchange, String id, int ack) throws IOException {
        try {
            ReplicaValue value = replicas.readData(ack, id);

            if (value == null || value.deleted()) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_NOT_FOUND, -1);
                return;
            }

            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, value.value().length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(value.value());
            }
        } catch (IllegalStateException e) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_UNAVAILABLE, -1);
        }
    }

    private void processPut(HttpExchange exchange, String id, int ack) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        int success = replicas.writeData(id, body);

        if (success < ack) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_UNAVAILABLE, -1);
            return;
        }

        exchange.sendResponseHeaders(HttpURLConnection.HTTP_CREATED, -1);
    }

    private void processDelete(HttpExchange exchange, String id, int ack) throws IOException {
        int success = replicas.deleteData(id);

        if (success < ack) {
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_UNAVAILABLE, -1);
            return;
        }

        exchange.sendResponseHeaders(HttpURLConnection.HTTP_ACCEPTED, -1);
    }

    public void disableReplica(int id) {
        replicas.disableNode(id);
    }

    public void enableReplica(int id) {
        replicas.enableNode(id);
    }

    public Replicas getReplicas() {
        return replicas;
    }
}
