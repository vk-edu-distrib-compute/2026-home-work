package company.vk.edu.distrib.compute.patersss;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DumpStatusHandler implements HttpHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(DumpStatusHandler.class);

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            String method = exchange.getRequestMethod();
            LOGGER.debug("Handling /v0/status method={}", method);

            if (!"GET".equals(method)) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            exchange.sendResponseHeaders(200, -1);
        }
    }
}
