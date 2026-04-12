package company.vk.edu.distrib.compute.usl;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.util.concurrent.locks.ReentrantLock;

final class UslNodeServer {
    private static final Logger log = LoggerFactory.getLogger(UslNodeServer.class);

    private static final String STATUS_PATH = "/v0/status";
    private static final String ENTITY_PATH = "/v0/entity";

    private final int port;
    private final String endpoint;
    private final Dao<byte[]> dao;
    private final StatusHttpHandler statusHandler = new StatusHttpHandler();
    private final EntityHttpHandler entityHandler;
    private final ReentrantLock lifecycleLock = new ReentrantLock();

    private HttpServer server;

    UslNodeServer(int port, Dao<byte[]> dao) {
        this.port = port;
        this.endpoint = endpoint(port);
        this.dao = dao;
        this.entityHandler = new EntityHttpHandler(dao);
    }

    static String endpoint(int port) {
        return "http://localhost:" + port;
    }

    String endpoint() {
        return endpoint;
    }

    void start() {
        lifecycleLock.lock();
        try {
            if (server != null) {
                return;
            }

            try {
                HttpServer createdServer = HttpServer.create(new InetSocketAddress("localhost", port), 0);
                createdServer.createContext(STATUS_PATH, statusHandler);
                createdServer.createContext(ENTITY_PATH, entityHandler);
                createdServer.start();
                server = createdServer;
                log.info("Node started on {}", endpoint);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to start node on " + endpoint, e);
            }
        } finally {
            lifecycleLock.unlock();
        }
    }

    void stop() {
        lifecycleLock.lock();
        try {
            if (server == null) {
                return;
            }

            server.stop(0);
            server = null;
            closeDao();
            log.info("Node stopped on {}", endpoint);
        } finally {
            lifecycleLock.unlock();
        }
    }

    private void closeDao() {
        try {
            dao.close();
        } catch (IOException e) {
            log.warn("Failed to close dao for {}", endpoint, e);
        }
    }
}
