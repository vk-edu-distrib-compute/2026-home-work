package company.vk.edu.distrib.compute.danila0111;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

public class MyKVService implements KVService {

    private final int port;
    private HttpServer server;

    private final Map<String, byte[]> storage = new ConcurrentHashMap<>();

    private static final String FILE_NAME = "data.db";

    public MyKVService(int port) {
        this.port = port;
    }

    @Override
    public void start() {
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);

            server.createContext("/v0/status", this::handleStatus);
            server.createContext("/v0/entity", this::handleEntity);

            server.setExecutor(Executors.newFixedThreadPool(4));

            loadFromFile();

            server.start();

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    private void loadFromFile() {
        java.io.File file = new java.io.File(FILE_NAME);
        if (!file.exists()) {
            return;
        }

        try (var fis = java.nio.file.Files.newInputStream(file.toPath())) {
            byte[] intBuf = new byte[4];

            while (fis.read(intBuf) == 4) {
                int keyLen = java.nio.ByteBuffer.wrap(intBuf).getInt();

                byte[] keyBytes = fis.readNBytes(keyLen);
                String key = new String(keyBytes);

                fis.read(intBuf);
                int valLen = java.nio.ByteBuffer.wrap(intBuf).getInt();

                byte[] value = fis.readNBytes(valLen);

                storage.put(key, value);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }

        byte[] response = "OK".getBytes();
        exchange.sendResponseHeaders(200, response.length);

        try (OutputStream os = exchange.getResponseBody()) {
            os.write(response);
        }
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        String id = extractId(exchange.getRequestURI().getQuery());

        if (id == null || id.isEmpty()) {
            exchange.sendResponseHeaders(400, -1);
            exchange.close();
            return;
        }

        String method = exchange.getRequestMethod();

        switch (method) {
            case "GET":
                handleGet(exchange, id);
                break;
            case "PUT":
                handlePut(exchange, id);
                break;
            case "DELETE":
                handleDelete(exchange, id);
                break;
            default:
                exchange.sendResponseHeaders(405, -1);
                exchange.close();
        }
    }

    private String extractId(String query) {
        if (query == null) {
            return null;
        }

        for (String param : query.split("&")) {
            if (param.startsWith("id=")) {
                return param.substring(3);
            }
        }
        return null;
    }

    private void handleGet(HttpExchange exchange, String id) throws IOException {
    byte[] value = storage.get(id);

    if (value == null) {
        exchange.sendResponseHeaders(404, -1);
        exchange.close();
        return;
    }

    exchange.sendResponseHeaders(200, value.length);
    try (OutputStream os = exchange.getResponseBody()) {
        os.write(value);
    }
    }

    private void handlePut(HttpExchange exchange, String id) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();

        storage.put(id, body);

        try (var fos = java.nio.file.Files.newOutputStream(
        java.nio.file.Path.of(FILE_NAME),
        java.nio.file.StandardOpenOption.CREATE,
        java.nio.file.StandardOpenOption.APPEND)) {
            byte[] keyBytes = id.getBytes();

            fos.write(intToBytes(keyBytes.length));
            fos.write(keyBytes);

            fos.write(intToBytes(body.length));
            fos.write(body);
        }

        exchange.sendResponseHeaders(201, -1);
        exchange.close();
    }

    private byte[] intToBytes(int value) {
        return java.nio.ByteBuffer.allocate(4).putInt(value).array();
    }

    private void handleDelete(HttpExchange exchange, String id) throws IOException {
    storage.remove(id);

    exchange.sendResponseHeaders(202, -1);
    exchange.close();
    }
}
