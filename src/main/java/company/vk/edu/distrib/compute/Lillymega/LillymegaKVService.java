//реализация интерфейса
package company.vk.edu.distrib.compute.Lillymega;

import  com.sun.net.httpserver.HttpServer;
import  com.sun.net.httpserver.HttpExchange;
import  company.vk.edu.distrib.compute.KVService;
import  company.vk.edu.distrib.compute.LillymegaDao;

import java.io.IOException;
import java.net.InetSocketAddress;

public class LillymegaKVService implements KVService{
    private final HttpServer server;
    private final Dao<byte[]> dao;
    //конструктор
    public LillymegaKVService(int port, Dao<byte[]> dao) throws IOException {
        this.dao = dao;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.server.createContext("/v0/status", this::handleStatus);
        this.server.createContext("/v0/entity", this::handleEntity);
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            exchange.close();
            return;
        }
        exchange.sendResponseHeaders(200, -1);
        exchange.close();
    }
    /**
     * Bind storage to HTTP port and start listening.
     *
     * <p>
     * May be called only once.
     */
    @Override
    public void start() {
        server.start();
    }


    /**
     * Stop listening and free all the resources.
     *
     * <p>
     * May be called only once and after {@link #start()}.
     */
    @Override
    public void stop() {
        server.stop(0);
    }
}
