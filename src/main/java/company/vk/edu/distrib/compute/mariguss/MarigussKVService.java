package company.vk.edu.distrib.compute.mariguss;

import company.vk.edu.distrib.compute.KVService;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

public class MarigussKVService implements KVService {
    private final HttpServer server;
    private final MarigussDao dao;
    
    public MarigussKVService(int port) throws IOException {
        this.dao = new MarigussDao();
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        
        server.createContext("/v0/status", this::handleStatus);
        server.createContext("/v0/entity", this::handleEntity);
        server.setExecutor(null);
    }
    
    private void handleStatus(HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(200, 0);
        exchange.close();
    }
    
    private void handleEntity(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String query = exchange.getRequestURI().getQuery();
        String key = extractKey(query);
        
        if (key == null || key.isEmpty()) {
            exchange.sendResponseHeaders(400, 0);
            exchange.close();
            return;
        }
        
        try {
            switch (method) {
                case "GET":
                    handleGet(exchange, key);
                    break;
                case "PUT":
                    handlePut(exchange, key);
                    break;
                case "DELETE":
                    handleDelete(exchange, key);
                    break;
                default:
                    exchange.sendResponseHeaders(400, 0);
                    exchange.close();
            }
        } catch (Exception e) {
            exchange.sendResponseHeaders(500, 0);
            exchange.close();
        }
    }
    
    private void handleGet(HttpExchange exchange, String key) throws IOException {
        try {
            byte[] value = dao.get(key);
            exchange.sendResponseHeaders(200, value.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(value);
            }
        } catch (Exception e) {
            exchange.sendResponseHeaders(404, 0);
        }
        exchange.close();
    }
    
    private void handlePut(HttpExchange exchange, String key) throws IOException {
        try {
            byte[] body = exchange.getRequestBody().readAllBytes();
            dao.upsert(key, body);
            exchange.sendResponseHeaders(201, 0);
        } catch (Exception e) {
            exchange.sendResponseHeaders(400, 0);
        }
        exchange.close();
    }
    
    private void handleDelete(HttpExchange exchange, String key) throws IOException {
        try {
            dao.delete(key);
            exchange.sendResponseHeaders(202, 0);
        } catch (Exception e) {
            exchange.sendResponseHeaders(400, 0);
        }
        exchange.close();
    }
    
    private String extractKey(String query) {
        if (query == null || query.isEmpty()) {
            return null;
        }
        if (!query.startsWith("id=")) {
            return null;
        }
        String key = query.substring(3);
        return key.isEmpty() ? null : key;
    }
    
    @Override
    public void start() {
        server.start();
    }
    
    @Override
    public void stop() {
        server.stop(0);
    }
}
