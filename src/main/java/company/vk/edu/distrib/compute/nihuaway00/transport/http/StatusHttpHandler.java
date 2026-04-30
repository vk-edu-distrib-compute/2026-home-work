package company.vk.edu.distrib.compute.nihuaway00.transport.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.OutputStream;

public class StatusHttpHandler implements HttpHandler {

    public StatusHttpHandler() {

    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        byte[] body = "{\"status\": \"ok\"}".getBytes();

        try (exchange) {
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        }
    }
}
