package company.vk.edu.distrib.compute.linempy;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;

/**
 * KVServiceImpl — обработчик http запросов.
 *
 * @author Linempy
 * @since 28.03.2026
 */
public class KVServiceImpl implements KVService {

    private static final Logger log = LoggerFactory.getLogger(KVServiceImpl.class);
    private static final int BACKLOG = 0;

    private final HttpServer server;
    private final Dao<byte[]> dao;

    public KVServiceImpl(Dao<byte[]> dao, int port) throws IOException {
        this.dao = dao;
        this.server = HttpServer.create(new InetSocketAddress(port),BACKLOG);

        server.createContext("/v0/status", this::statusHandlerCheck);
        server.createContext("/v0/entity", this::dispatcherEntityHandler);

        log.info("Server created on port {}", port);
    }

    private void statusHandlerCheck(HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(HttpCodes.OK, -1);
        exchange.close();
    }

    @SuppressWarnings("PMD.UseTryWithResources")
    private void dispatcherEntityHandler(HttpExchange exchange) throws IOException {
        try {
            entityHandler(exchange);
        } catch (NoSuchElementException e) {
            exchange.sendResponseHeaders(HttpCodes.NOT_FOUND, -1);
        } catch (IllegalArgumentException e) {
            exchange.sendResponseHeaders(HttpCodes.BAD_REQUEST, -1);
        } catch (Exception e) {
            exchange.sendResponseHeaders(HttpCodes.SERVER_ERROR, -1);
        } finally {
            exchange.close();
        }
    }

    protected void entityHandler(HttpExchange exchange) throws IOException {
        String id = detachedId(exchange.getRequestURI().getQuery());

        switch (exchange.getRequestMethod()) {
            case "GET" -> getById(exchange, id);
            case "PUT" -> createById(exchange, id);
            case "DELETE" -> deleteById(exchange, id);
            default -> exchange.sendResponseHeaders(HttpCodes.METHOD_NOT_ALLOWED, -1);
        }
    }

    protected void deleteById(HttpExchange exchange, String id) throws IOException {
        dao.delete(id);
        exchange.sendResponseHeaders(HttpCodes.ACCEPTED, -1);
    }

    protected void getById(HttpExchange exchange, String id) throws IOException {
        byte[] result = dao.get(id);
        exchange.sendResponseHeaders(HttpCodes.OK, result.length);
        exchange.getResponseBody().write(result);
    }

    protected void createById(HttpExchange exchange, String id) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        dao.upsert(id, body);
        exchange.sendResponseHeaders(HttpCodes.CREATED, -1);
    }

    protected String detachedId(String query) {
        if (query.isBlank()) {
            throw new IllegalArgumentException("Пропущены query-параметры");
        }

        String[] params = query.split("&");
        for (String param : params) {
            String[] pairs = param.split("=");
            if ("id".equals(pairs[0])) {
                if (pairs.length < 2 || pairs[1].isBlank()) {
                    throw new IllegalArgumentException("Параметр 'id' пустой");
                }
                return pairs[1];
            }
        }
        throw new IllegalArgumentException("Пропущен обязательный query-параметр 'id'");
    }

    @Override
    public void start() {
        server.start();
        log.info("Server started");
    }

    @Override
    public void stop() {
        server.stop(0);
        log.info("Server stopping");
    }
}
