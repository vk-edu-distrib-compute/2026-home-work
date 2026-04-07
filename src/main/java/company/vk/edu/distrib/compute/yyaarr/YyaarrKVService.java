package company.vk.edu.distrib.compute.yyaarr;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

public class YyaarrKVService implements KVService {
    private static final Logger LOGGER = LoggerFactory.getLogger(YyaarrKVService.class);
    private final HttpServer server;
    private final Dao<byte[]> dao;

    YyaarrKVService(int port, Dao<byte[]> dao) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = dao;
        initServer();

    }

    private void initServer() {
        server.createContext("/v0/status", new YyaarrHandlers.StatusHandler());
        server.createContext("/v0/entity", new YyaarrHandlers.EntityHandler(dao, LOGGER));
    }

    @Override
    public void start() {
        LOGGER.info("Service starting");
        server.start();
        LOGGER.info("Service started");

    }

    @Override
    public void stop() {
        server.stop(0);
        LOGGER.info("Service stopped");

    }

}

