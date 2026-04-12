package company.vk.edu.distrib.compute.maryarta;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;

public class KVServiceImpl implements KVService {
    private final HttpServer server;
    private final Dao<byte[]> dao;

    public KVServiceImpl(int port) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = new FileDao();
        createContext();
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
    }

    public void createContext() {
        server.createContext("/v0/status", http -> {
            String method = http.getRequestMethod();
            if ("GET".equals(method)) {
                http.sendResponseHeaders(200,-1);
            } else {
                http.sendResponseHeaders(405,-1);
            }
            http.close();
        });
        server.createContext("/v0/entity", http -> {
            String method = http.getRequestMethod();
            String query = http.getRequestURI().getQuery();
            try {
                switch (method) {
                    case "GET" -> {
                        byte [] value = dao.get(parseId(query));
                        http.sendResponseHeaders(200, value.length); // OK
                        http.getResponseBody().write(value);
                    }
                    case "PUT" -> {
                          byte[] newValue = http.getRequestBody().readAllBytes();
                          dao.upsert(parseId(query), newValue);
                          http.sendResponseHeaders(201, 0); // OK
                    }
                    case "DELETE" -> {
                        dao.delete(parseId(query));
                        http.sendResponseHeaders(202, 0);
                    }
                default -> http.sendResponseHeaders(405, 0);
                }
            } catch (IllegalArgumentException e) {
                http.sendResponseHeaders(400, 0); // Bad Request
            } catch (NoSuchElementException e) {
                http.sendResponseHeaders(404, 0); // Not Found
            }
            http.close();
        });
    }

    private static String parseId(String query) {
        if (query != null && query.startsWith("id=")) {
            return query.substring(3);
        } else {
            throw new IllegalArgumentException("Bad query");
        }
    }
}
