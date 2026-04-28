package company.vk.edu.distrib.compute.martinez1337.controller;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.NoSuchElementException;

import static company.vk.edu.distrib.compute.martinez1337.controller.ResponseStatus.*;

public abstract class BaseHttpHandler implements HttpHandler {
    protected final Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public final void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            try {
                switch (exchange.getRequestMethod()) {
                    case "GET" -> handleGet(exchange);
                    case "PUT" -> handlePut(exchange);
                    case "DELETE" -> handleDelete(exchange);
                    default -> exchange.sendResponseHeaders(405, 0);
                }
            } catch (IllegalArgumentException e) {
                sendError(exchange, BAD_REQUEST);
            } catch (NoSuchElementException e) {
                sendError(exchange, NOT_FOUND);
            } catch (UnsupportedOperationException e) {
                sendError(exchange, METHOD_NOT_ALLOWED);
            } catch (Exception e) {
                sendError(exchange, INTERNAL_ERROR);
            }
        }
    }

    protected void handleGet(HttpExchange exchange) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    protected void handlePut(HttpExchange exchange) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    protected void handleDelete(HttpExchange exchange) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    protected final void sendError(HttpExchange exchange, ResponseStatus status) throws IOException {
        if (log.isErrorEnabled()) {
            log.error(status.getMessage());
        }
        exchange.sendResponseHeaders(status.getCode(), 0);
    }
}
