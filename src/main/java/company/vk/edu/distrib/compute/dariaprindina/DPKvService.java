package company.vk.edu.distrib.compute.dariaprindina;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;
import java.util.Objects;

public class DPKvService implements KVService {
    private static final Logger log = LoggerFactory.getLogger(DPKvService.class);
    private static final String ID_PARAM_PREFIX = "id=";

    private final HttpServer server;
    private final Dao<byte[]> dao;

    public DPKvService(int port, Dao<byte[]> dao) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = dao;
        initServer();
    }

    private void initServer() {
        server.createContext("/v0/status", new ErrorHttpHandler(httpExchange -> {
            final var method = httpExchange.getRequestMethod();
            if (Objects.equals("GET", method)) {
                sendResponse(httpExchange, 200, null);
            } else {
                sendResponse(httpExchange, 405, null);
            }
        }));

        server.createContext("/v0/entity", new ErrorHttpHandler(httpExchange -> {
            final var method = httpExchange.getRequestMethod();
            final var query = httpExchange.getRequestURI().getQuery();
            final var id = parseId(query);
            if ("GET".equals(method)) {
                final var value = dao.get(id);
                sendResponse(httpExchange, 200, value);
            } else if ("PUT".equals(method)) {
                try (var requestBody = httpExchange.getRequestBody()) {
                    dao.upsert(id, requestBody.readAllBytes());
                }
                sendResponse(httpExchange, 201, null);
            } else if ("DELETE".equals(method)) {
                dao.delete(id);
                sendResponse(httpExchange, 202, null);
            } else {
                sendResponse(httpExchange, 405, null);
            }
        }));
    }

    private static String parseId(String query) {
        if (query != null && query.startsWith(ID_PARAM_PREFIX) && query.length() > ID_PARAM_PREFIX.length()) {
            return query.substring(ID_PARAM_PREFIX.length());
        }
        throw new IllegalArgumentException("bad query");
    }

    @Override
    public void start() {
        log.info("Starting");
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
        log.info("Stopped");
    }

    private static void sendResponse(HttpExchange exchange, int code, byte[] body) throws IOException {
        final byte[] responseBody = body == null ? new byte[0] : body;
        exchange.sendResponseHeaders(code, responseBody.length);
        if (responseBody.length > 0) {
            try (OutputStream outputStream = exchange.getResponseBody()) {
                outputStream.write(responseBody);
            }
            return;
        }
        exchange.getResponseBody().close();
    }

    private static final class ErrorHttpHandler implements HttpHandler {
        private final HttpHandler delegate;

        private ErrorHttpHandler(HttpHandler delegate) {
            this.delegate = delegate;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                delegate.handle(exchange);
            } catch (IllegalArgumentException e) {
                sendResponse(exchange, 400, null);
            } catch (NoSuchElementException e) {
                sendResponse(exchange, 404, null);
            } catch (IOException e) {
                sendResponse(exchange, 500, null);
            }
        }
    }
}
