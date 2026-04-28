package company.vk.edu.distrib.compute.usl;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;

final class StatusHttpHandler implements HttpHandler {
    private static final String STATUS_PATH = "/v0/status";
    private static final String GET_METHOD = "GET";

    @Override
    @SuppressWarnings("PMD.UseTryWithResources")
    public void handle(HttpExchange exchange) throws IOException {
        try {
            if (!STATUS_PATH.equals(exchange.getRequestURI().getPath())) {
                ExchangeResponses.sendEmpty(exchange, 404);
                return;
            }

            if (!GET_METHOD.equals(exchange.getRequestMethod())) {
                ExchangeResponses.sendEmpty(exchange, 405);
                return;
            }

            ExchangeResponses.sendEmpty(exchange, 200);
        } finally {
            exchange.close();
        }
    }
}
