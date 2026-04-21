package company.vk.edu.distrib.compute.usl;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.usl.sharding.ShardingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

final class UslNodeServer {
    private static final Logger log = LoggerFactory.getLogger(UslNodeServer.class);
    private static final int EXECUTOR_THREADS = 8;

    private static final String STATUS_PATH = "/v0/status";
    private static final String ENTITY_PATH = "/v0/entity";

    private final int port;
    private final String localEndpointUrl;
    private final Dao<byte[]> dao;
    private final StatusHttpHandler statusHandler = new StatusHttpHandler();
    private final EntityHttpHandler entityHandler;
    private final ReentrantLock lifecycleLock = new ReentrantLock();

    private HttpServer server;
    private ExecutorService executorService;
    private boolean started;

    UslNodeServer(int port, Dao<byte[]> dao) {
        this(port, dao, null, null);
    }

    UslNodeServer(int port, Dao<byte[]> dao, ShardingStrategy shardingStrategy, HttpClient httpClient) {
        this.port = port;
        this.localEndpointUrl = endpointUrl(port);
        this.dao = dao;
        this.entityHandler = new EntityHttpHandler(
            dao,
            localEndpointUrl,
            shardingStrategy,
            httpClient == null ? null : new ClusterRequestProxy(httpClient)
        );
    }

    static String endpointUrl(int port) {
        return "http://localhost:" + port;
    }

    String endpoint() {
        return localEndpointUrl;
    }

    void start() {
        lifecycleLock.lock();
        try {
            if (started) {
                return;
            }

            try {
                HttpServer createdServer = HttpServer.create(new InetSocketAddress("localhost", port), 0);
                ExecutorService createdExecutor = Executors.newFixedThreadPool(EXECUTOR_THREADS);
                createdServer.setExecutor(createdExecutor);
                createdServer.createContext(STATUS_PATH, statusHandler);
                createdServer.createContext(ENTITY_PATH, entityHandler);
                createdServer.start();
                server = createdServer;
                executorService = createdExecutor;
                started = true;
                log.info("Node started on {}", localEndpointUrl);
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to start node on " + localEndpointUrl, e);
            }
        } finally {
            lifecycleLock.unlock();
        }
    }

    void stop() {
        lifecycleLock.lock();
        try {
            if (!started) {
                return;
            }

            server.stop(0);
            stopExecutor();
            server = null;
            started = false;
            closeDao();
            log.info("Node stopped on {}", localEndpointUrl);
        } finally {
            lifecycleLock.unlock();
        }
    }

    private void stopExecutor() {
        if (executorService == null) {
            return;
        }

        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(1, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executorService.shutdownNow();
        } finally {
            executorService = null;
        }
    }

    private void closeDao() {
        try {
            dao.close();
        } catch (IOException e) {
            log.warn("Failed to close dao for {}", localEndpointUrl, e);
        }
    }
}
