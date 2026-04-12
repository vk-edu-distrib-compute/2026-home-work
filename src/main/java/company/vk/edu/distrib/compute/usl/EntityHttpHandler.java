package company.vk.edu.distrib.compute.usl;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

final class EntityHttpHandler implements HttpHandler {
    private static final String ENTITY_PATH = "/v0/entity";

    private final Dao<byte[]> dao;

    EntityHttpHandler(Dao<byte[]> dao) {
        this.dao = dao;
    }

    @Override
    @SuppressWarnings("PMD.UseTryWithResources")
    public void handle(HttpExchange exchange) throws IOException {
        try {
            if (!ENTITY_PATH.equals(exchange.getRequestURI().getPath())) {
                ExchangeResponses.sendEmpty(exchange, 404);
                return;
            }

            String key = extractId(exchange.getRequestURI());
            byte[] requestBody = readBody(exchange);
            handleLocal(exchange, key, requestBody);
        } catch (IllegalArgumentException e) {
            ExchangeResponses.sendEmpty(exchange, 400);
        } catch (NoSuchElementException e) {
            ExchangeResponses.sendEmpty(exchange, 404);
        } catch (IOException e) {
            ExchangeResponses.sendEmpty(exchange, 503);
        } catch (Exception e) {
            ExchangeResponses.sendEmpty(exchange, 500);
        } finally {
            exchange.close();
        }
    }

    private void handleLocal(HttpExchange exchange, String key, byte[] requestBody) throws IOException {
        switch (exchange.getRequestMethod()) {
            case "GET" -> ExchangeResponses.sendBody(exchange, 200, dao.get(key));
            case "PUT" -> handlePut(exchange, key, requestBody);
            case "DELETE" -> handleDelete(exchange, key);
            default -> ExchangeResponses.sendEmpty(exchange, 405);
        }
    }

    private void handlePut(HttpExchange exchange, String key, byte[] requestBody) throws IOException {
        dao.upsert(key, requestBody);
        ExchangeResponses.sendEmpty(exchange, 201);
    }

    private void handleDelete(HttpExchange exchange, String key) throws IOException {
        dao.delete(key);
        ExchangeResponses.sendEmpty(exchange, 202);
    }

    private static byte[] readBody(HttpExchange exchange) throws IOException {
        return switch (exchange.getRequestMethod()) {
            case "PUT" -> exchange.getRequestBody().readAllBytes();
            case "GET", "DELETE" -> new byte[0];
            default -> new byte[0];
        };
    }

    private static String extractId(URI requestUri) {
        String query = requestUri.getRawQuery();
        if (query == null || query.isEmpty()) {
            throw new IllegalArgumentException("Missing id query parameter");
        }

        String id = null;
        for (String parameter : query.split("&")) {
            int delimiterIndex = parameter.indexOf('=');
            String rawName = delimiterIndex < 0 ? parameter : parameter.substring(0, delimiterIndex);
            if (!"id".equals(URLDecoder.decode(rawName, StandardCharsets.UTF_8))) {
                continue;
            }

            if (id != null) {
                throw new IllegalArgumentException("Duplicate id query parameter");
            }

            String rawValue = delimiterIndex < 0 ? "" : parameter.substring(delimiterIndex + 1);
            id = URLDecoder.decode(rawValue, StandardCharsets.UTF_8);
        }

        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("Empty id query parameter");
        }

        return id;
    }
}
