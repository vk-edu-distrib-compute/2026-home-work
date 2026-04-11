package company.vk.edu.distrib.compute.patersss;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

public class DumpKVService implements KVService {
    public static final Logger logger = LoggerFactory.getLogger(DumpKVService.class);
    private final HttpServer server;
    private final Dao<byte[]> dao;

    public DumpKVService(int port, Dao<byte[]> dao) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = dao;
        setupServer();
    }

    private void setupServer() {
        this.server.createContext("/v0/status", http -> {
            final var method = http.getRequestMethod();

            if ("GET".equals(method)) http.sendResponseHeaders(200, -1);
            else http.sendResponseHeaders(405, -1);

            http.close();
        });

        this.server.createContext("/v0/entity", http -> {
            final var method = http.getRequestMethod();
            final var query = http.getRequestURI().getQuery();
            final var entityId = UrlParamsUtils.getIdFromQuery(query);
            switch (method) {
                case "GET" -> {
                    final var value = dao.get(entityId);
                    http.sendResponseHeaders(200, value.length);
                    http.getResponseBody().write(value);
                }
                case "PUT" -> {

                }
                case "DELETE" -> {

                }
                default -> http.sendResponseHeaders(405, -1);
            }
            http.close();
        });
    }

    @Override
    public void start() {
        logger.info("Server is starting");
        this.server.start();
        logger.info("Server started");
    }

    @Override
    public void stop() {
        logger.info("Server is stopping(10 sec to graceful shutdown");
        this.server.stop(10);
        logger.info("Server stopped");
    }

    private static class Test implements HttpHandler {

        @Override
        public void handle(HttpExchange exchange) throws IOException {

        }
    }
}
