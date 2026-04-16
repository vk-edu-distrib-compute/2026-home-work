package company.vk.edu.distrib.compute.teeaamma;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;
import java.util.Objects;

public class TeeaammaKVService implements KVService {

    private static final Logger log = LoggerFactory.getLogger(TeeaammaKVService.class);
    private static final String ID_PARAM_PREFIX = "id=";

    private final HttpServer server;
    private final Dao<byte[]> dao;

    public TeeaammaKVService(int port, Dao<byte[]> dao) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = dao;
        initServer();
    }

    private void initServer() {
        server.createContext("/v0/status", http -> {
            final var method = http.getRequestMethod();
            if (Objects.equals("GET", method)) {
                http.sendResponseHeaders(200, 0);
            } else {
                http.sendResponseHeaders(503, 0);
            }
            http.close();
        });

        server.createContext("/v0/entity", new ErrorHttpHandler(http -> {
            final var method = http.getRequestMethod();
            final var query = http.getRequestURI().getQuery();
            final var id = parseId(query);
            switch (method) {
                case "GET" -> {
                    final var value = dao.get(id);
                    http.sendResponseHeaders(200, value.length);
                    try (var responseBody = http.getResponseBody()) {
                        responseBody.write(value);
                    }
                }
                case "PUT" -> {
                    final var bytes = http.getRequestBody().readAllBytes();
                    dao.upsert(id, bytes);
                    http.sendResponseHeaders(201, 0);
                }
                case "DELETE" -> {
                    dao.delete(id);
                    http.sendResponseHeaders(202, 0);
                }
                default -> http.sendResponseHeaders(405, 0);
            }
        }));
    }

    private static String parseId(String query) {
        if (query.startsWith(ID_PARAM_PREFIX)) {
            return query.substring(ID_PARAM_PREFIX.length());
        } else {
            throw new IllegalArgumentException("bad query");
        }
    }

    @Override
    public void start() {
        log.info("Starting");
        server.start();
    }

    @Override
    public void stop() {
        server.stop(1);
        log.info("Stopped");
    }

    private static final class ErrorHttpHandler implements HttpHandler {

        private final HttpHandler delegate;

        private ErrorHttpHandler(HttpHandler delegate) {
            this.delegate = delegate;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try (exchange) {
                try {
                    delegate.handle(exchange);
                } catch (IllegalArgumentException e) {
                    exchange.sendResponseHeaders(400, 0);
                } catch (NoSuchElementException e) {
                    exchange.sendResponseHeaders(404, 0);
                } catch (IOException e) {
                    exchange.sendResponseHeaders(500, 0);
                }
            }
        }
    }
}
