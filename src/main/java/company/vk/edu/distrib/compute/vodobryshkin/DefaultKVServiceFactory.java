package company.vk.edu.distrib.compute.vodobryshkin;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.vodobryshkin.handlers.EntityHandler;
import company.vk.edu.distrib.compute.vodobryshkin.handlers.StatusHandler;

import java.io.IOException;
import java.net.InetSocketAddress;

public class DefaultKVServiceFactory extends KVServiceFactory {
    private static final int BACKLOG_SIZE = 128;

    private final Dao<byte[]> storage;

    public DefaultKVServiceFactory() throws IOException {
        this(new FileDao());
    }

    public DefaultKVServiceFactory(Dao<byte[]> storage) {
        super();
        this.storage = storage;
    }

    @Override
    public KVService doCreate(int port) throws IOException {
        HttpServer httpServer = HttpServer.create(new InetSocketAddress(port), BACKLOG_SIZE);

        httpServer.createContext("/v0/status", new StatusHandler());
        httpServer.createContext("/v0/entity", new EntityHandler(storage));

        return new DefaultKVService(httpServer);
    }
}
