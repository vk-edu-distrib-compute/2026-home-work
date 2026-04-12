package company.vk.edu.distrib.compute.patersss;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

public class DumpKVService implements KVService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DumpKVService.class);

    private final HttpServer server;
    private final Dao<byte[]> dao;

    public DumpKVService(int port, Dao<byte[]> dao) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = dao;
        setupServer();
    }

    private void setupServer() {
        server.createContext("/v0/status", new DumpStatusHandler());
        server.createContext("/v0/entity", new DumpEntityHandler(dao));
    }

    @Override
    public void start() {
        LOGGER.info("Server is starting");
        server.start();
        LOGGER.info("Server started");
    }

    @Override
    public void stop() {
        LOGGER.info("Server is stopping");
        try {
            server.stop(1);
            dao.close();
            LOGGER.info("Server stopped");
        } catch (IOException e) {
            LOGGER.error("Failed to close dao", e);
        }
    }
}
