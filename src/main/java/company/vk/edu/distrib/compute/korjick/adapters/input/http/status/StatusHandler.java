package company.vk.edu.distrib.compute.korjick.adapters.input.http.status;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.korjick.adapters.input.http.Constants;

import java.io.IOException;

public class StatusHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
        final var method = httpExchange.getRequestMethod();
        switch (method) {
            case Constants.HTTP_METHOD_GET -> httpExchange.sendResponseHeaders(
                    Constants.HTTP_STATUS_OK,
                    Constants.EMPTY_BODY_LENGTH
            );
            case null, default -> httpExchange.sendResponseHeaders(
                    Constants.HTTP_STATUS_METHOD_NOT_ALLOWED,
                    Constants.EMPTY_BODY_LENGTH
            );
        }
    }
}
