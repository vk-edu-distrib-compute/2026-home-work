package company.vk.edu.distrib.compute.kuznetsovasvetlana6;

import com.sun.net.httpserver.HttpServer;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import java.io.IOException;
import java.net.InetSocketAddress;

public class MyKVService implements KVService {
    private final HttpServer server;
    private final Dao<byte[]> dao;

    public MyKVService(int port, Dao<byte[]> dao) throws IOException {
        this.dao = dao;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);

        server.createContext("/v0/status", exchange -> {
            exchange.sendResponseHeaders(200, 0);
            exchange.close();
        });

        server.createContext("/v0/entity", new EntityHandler(dao));
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
        try {
            dao.close();
        } catch (IOException e) {
            // Игнорируем
        }
    }
}
