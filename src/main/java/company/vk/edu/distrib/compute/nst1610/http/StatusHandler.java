package company.vk.edu.distrib.compute.nst1610.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.util.Objects;

public class StatusHandler implements HttpHandler {

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!Objects.equals(exchange.getRequestMethod(), "GET")) {
                exchange.sendResponseHeaders(405, 0);
                return;
            }
            exchange.sendResponseHeaders(200, 0);
        }
    }
}
