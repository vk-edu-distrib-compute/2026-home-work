package company.vk.edu.distrib.compute.yyaarr;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

public class YyaarrKVServiceFile implements KVService {
    private static final Logger LOGGER = LoggerFactory.getLogger(YyaarrKVServiceFile.class);
    private HttpServer server;
    private final Dao<byte[]> dao;
    private final int port;
    private final YyaarrKVCluster cluster;
    private boolean isRunning;

    YyaarrKVServiceFile(int port, Dao<byte[]> dao, YyaarrKVCluster cluster) throws IOException {
        this.port = port;
        this.dao = dao;
        this.cluster = cluster;
        initServer();
    }

    YyaarrKVServiceFile(int port, Dao<byte[]> dao) throws IOException {
        this(port, dao, null);
    }

    private void initServer() throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/v0/status", new YyaarrHandlers.StatusHandler());
        if (cluster != null) {
            server.createContext("/v0/entity",
                    new YyaarrHandlers.EntityHandler(dao, LOGGER, cluster, port));
        } else {
            server.createContext("/v0/entity",
                    new YyaarrHandlers.EntityHandler(dao, LOGGER));
        }
    }

    @Override
    public void start() {
        LOGGER.info("Service starting");
        if (isRunning) {
            return;
        }
        if (server == null) {
            try {
                initServer();
            } catch (IOException e) {
                throw new IllegalStateException("Failed to initialize server on port " + port, e);
            }
        }
        server.start();
        isRunning = true;
        LOGGER.info("Service started on port {}", port);
        LOGGER.info("Service started");
    }

    @Override
    @SuppressWarnings("AssignmentToNull")
    public void stop() {
        if (!isRunning) {
            return;
        }
        server.stop(0);
        server = null; // need to reset for restart
        isRunning = false;
        LOGGER.info("Service stopped on port {}", port);
    }

    public int getPort() {
        return port;
    }

}

