package company.vk.edu.distrib.compute.martinez1337.service;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.martinez1337.controller.EntityHttpHandler;
import company.vk.edu.distrib.compute.martinez1337.controller.StatusHandler;

import java.io.IOException;
import java.net.InetSocketAddress;

public class InMemoryKVService implements KVService {
    private final Dao<byte[]> dao;
    private final HttpServer server;

    public InMemoryKVService(int port, Dao<byte[]> dao) throws IOException {
        this.dao = dao;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        initServer();
    }

    private void initServer() {
        server.createContext("/v0/status", new StatusHandler());
        server.createContext("/v0/entity", new EntityHttpHandler(dao));
    }

    @Override
    public void start() {
        this.server.start();
    }

    @Override
    public void stop() {
        this.server.stop(1);
    }
}
