package company.vk.edu.distrib.compute.nihuaway00.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.nihuaway00.replication.ReplicaManager;

import java.io.IOException;
import java.io.OutputStream;

public class PingHandler implements HttpHandler {

    private final ReplicaManager replicaManager;

    public PingHandler(ReplicaManager replicaManager) {
        this.replicaManager = replicaManager;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        boolean available = replicaManager.available();
        byte[] body = available
                ? "{\"status\": \"ok\"}".getBytes()
                : "{\"status\": \"not available\", \"desc\":\"entity dao not available\"}".getBytes();
        int status = available ? 200 : 503;

        try (exchange) {
            exchange.sendResponseHeaders(status, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        }
    }
}
