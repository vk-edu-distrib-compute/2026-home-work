package company.vk.edu.distrib.compute.vodobryshkin.handlers;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.vodobryshkin.HttpMethod;
import company.vk.edu.distrib.compute.vodobryshkin.StatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class StatusHandler implements HttpHandler {
    private static final Logger log = LoggerFactory.getLogger("server");

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();

        log.info("Received a {}-request on /v0/status endpoint", method);

        try {
            if (!method.equals(HttpMethod.GET.name())) {
                exchange.sendResponseHeaders(StatusCode.METHOD_NOT_ALLOWED.getCode(), -1);

                return;
            }

            exchange.sendResponseHeaders(StatusCode.OK.getCode(), -1);

            log.debug("Successfully handled a request on /v0/status endpoint");
        } catch (IOException e) {
            exchange.sendResponseHeaders(StatusCode.SERVICE_UNAVAILABLE.getCode(), -1);
        }

    }
}
