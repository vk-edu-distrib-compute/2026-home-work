package company.vk.edu.distrib.compute.golubtsov_pavel;

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

public class PGInMemoryKVService implements KVService {
    public static final String ID_PARAM_PREFIX = "id=";
    private static final Logger log = LoggerFactory.getLogger(PGInMemoryKVService.class);
    private final Dao<byte[]> dao;

    private final HttpServer server;

    public PGInMemoryKVService(int port, Dao<byte[]> dao) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = dao;
        initServer();
    }

    private void initServer() {
        server.createContext("/v0/status", new ErrorHttpHandler(http -> {
            final var method = http.getRequestMethod();
            if (Objects.equals("GET", method)) {
                http.sendResponseHeaders(200, 0);
            } else {
                http.sendResponseHeaders(405, 0);
            }
        }));

        server.createContext("/v0/entity", new ErrorHttpHandler(http -> {
            final var method = http.getRequestMethod();
            final var query = http.getRequestURI().getQuery();
            final var id = parseId(query);
            if (Objects.equals("GET", method)) {
                final var value = dao.get(id);
                http.sendResponseHeaders(200, value.length);
                http.getResponseBody().write(value);
            } else if (Objects.equals("PUT", method)) {
                byte[] body = http.getRequestBody().readAllBytes();
                dao.upsert(id, body);
                http.sendResponseHeaders(201, 0);
            } else if (Objects.equals("DELETE", method)) {
                dao.delete(id);
                http.sendResponseHeaders(202, 0);
            } else {
                http.sendResponseHeaders(405, 0);
            }

        }));
    }

    private static String parseId(String query) {
        if ((query == null) || (!query.startsWith(ID_PARAM_PREFIX))) {
            throw new IllegalArgumentException("bad query");
        }
        return query.substring(ID_PARAM_PREFIX.length());
    }

    @Override
    public void start() {
        log.info("Starting...");
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
            try {
                delegate.handle(exchange);
            } catch (IllegalArgumentException exp) {
                exchange.sendResponseHeaders(400, 0);
            } catch (NoSuchElementException exp) {
                exchange.sendResponseHeaders(404,0);
            } catch (IOException exp) {
                exchange.sendResponseHeaders(500,0);
            }
            exchange.close();
        }
    }
}
