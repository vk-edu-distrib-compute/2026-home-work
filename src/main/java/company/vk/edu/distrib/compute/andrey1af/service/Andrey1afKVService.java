package company.vk.edu.distrib.compute.andrey1af.service;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.andrey1af.controller.Andrey1afEntityHandler;
import company.vk.edu.distrib.compute.andrey1af.controller.Andrey1afStatusHandler;
import company.vk.edu.distrib.compute.andrey1af.sharding.HashRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

public class Andrey1afKVService implements KVService {

    private static final Logger log = LoggerFactory.getLogger(Andrey1afKVService.class);

    private final HttpServer server;
    private final Dao<byte[]> dao;
    private final String selfEndpoint;
    private final HashRouter hashRouter;

    Andrey1afKVService(int port, Dao<byte[]> dao) throws IOException {
        this(port, dao, null, null);
    }

    Andrey1afKVService(int port, Dao<byte[]> dao, String selfEndpoint, HashRouter hashRouter) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = dao;
        this.selfEndpoint = selfEndpoint;
        this.hashRouter = hashRouter;
        initServer();
    }

    private void initServer() {
        server.createContext("/v0/status", new Andrey1afStatusHandler());
        server.createContext("/v0/entity", new Andrey1afEntityHandler(dao, selfEndpoint, hashRouter));
    }

    @Override
    public void start() {
        server.start();
        if (log.isInfoEnabled()) {
            log.info("Starting Andrey1afKVService on port {}", server.getAddress().getPort());
        }
    }

    @Override
    public void stop() {
        server.stop(1);
        try {
            dao.close();
        } catch (IOException e) {
            log.error("Failed to close DAO", e);
        }
        if (log.isInfoEnabled()) {
            log.info("Andrey1afKVService stopped");
        }
    }
}
