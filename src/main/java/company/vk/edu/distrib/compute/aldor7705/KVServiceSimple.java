package company.vk.edu.distrib.compute.aldor7705;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.ReplicatedService;
import company.vk.edu.distrib.compute.aldor7705.handler.EntityHandler;
import company.vk.edu.distrib.compute.aldor7705.handler.StatusHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

public class KVServiceSimple implements ReplicatedService {
    private static final Logger log = LoggerFactory.getLogger(KVServiceSimple.class);
    private final int port;
    private final EntityDao dao;
    private final List<Integer> clusterPorts;
    private HttpServer httpServer;
    private boolean running;
    private final int replicas;

    public KVServiceSimple(int port, EntityDao dao, List<Integer> clusterPorts, int replicas) {
        this.port = port;
        this.dao = dao;
        this.clusterPorts = clusterPorts;
        this.replicas = replicas;
    }

    private HttpServer createServer() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/v0/status", new StatusHandler());
        server.createContext("/v0/entity", new EntityHandler(dao, port, clusterPorts));
        return server;
    }

    @Override
    public void start() {
        if (!running) {
            try {
                httpServer = createServer();
                log.info("Запуск сервиса на порту {}", port);
                httpServer.start();
                running = true;
            } catch (IOException e) {
                log.error("Ошибка при запуске", e);
            }
        }
    }

    @Override
    public void stop() {
        if (running) {
            log.info("Остановка сервиса на порту {}", port);
            httpServer.stop(0);
            running = false;
        }
    }

    @Override
    public int port() {
        return port;
    }

    @Override
    public int numberOfReplicas() {
        return replicas;
    }

    @Override
    public void disableReplica(int nodeId) {
        dao.disableReplica(nodeId);
    }

    @Override
    public void enableReplica(int nodeId) {
        dao.enableReplica(nodeId);
    }
}
