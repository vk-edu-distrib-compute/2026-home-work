package company.vk.edu.distrib.compute.tadzhnahal;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.net.InetSocketAddress;

public class TadzhnahalKVService implements KVService {
    private final int port;
    private final Dao<byte[]> dao;
    private HttpServer server;

    public TadzhnahalKVService(int port, Dao<byte[]> dao) {
        this.port = port;
        this.dao = dao;
    }

    @Override
    public void start() {
        if (server != null) {
            throw new IllegalStateException("Server already started");
        }

        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/v0/status", this::handleStatus);
            server.start();
        } catch (IOException e) {
            throw new IllegalStateException("Cannot start server", e);
        }
    }

    @Override
    public void stop() {
        if (server == null) {
            throw new IllegalStateException("Sever is not started");
        }

        server.stop(0);
        server = null;

        try {
            dao.close();
        }  catch (IOException e) {
            throw new IllegalStateException("Cannot close dao", e);
        }
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        try {
            if (!"GET".equals(exchange.getRequestMethod())) {
                sendEmptyResponse(exchange, 405);
                return;
            }

            sendEmptyResponse(exchange, 200);
        } finally {
            exchange.close();
        }
    }

    private void sendEmptyResponse(HttpExchange exchange, int code) throws IOException {
        exchange.sendResponseHeaders(code, -1);
    }
}
