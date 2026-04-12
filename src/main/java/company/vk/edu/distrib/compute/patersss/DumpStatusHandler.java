package company.vk.edu.distrib.compute.patersss;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

public class DumpStatusHandler implements HttpHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(DumpStatusHandler.class);
    public static final String GET_METHOD = "GET";

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            String method = exchange.getRequestMethod();
            LOGGER.debug("Handling /v0/status method={}", method);

            if (!Objects.equals(GET_METHOD, method)) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            exchange.sendResponseHeaders(200, -1);
        }
    }
}
