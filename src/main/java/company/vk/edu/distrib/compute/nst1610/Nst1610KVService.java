package company.vk.edu.distrib.compute.nst1610;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.nst1610.dao.FileDao;
import company.vk.edu.distrib.compute.nst1610.http.ClusterProxy;
import company.vk.edu.distrib.compute.nst1610.http.EntityHandler;
import company.vk.edu.distrib.compute.nst1610.http.StatusHandler;
import company.vk.edu.distrib.compute.nst1610.sharding.HashingStrategy;
import company.vk.edu.distrib.compute.nst1610.sharding.RendezvousHashingStrategy;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Nst1610KVService implements KVService {
    private static final Logger log = LoggerFactory.getLogger(Nst1610KVService.class);
    private final HttpServer server;
    private final Dao<byte[]> dao;
    private final String localEndpoint;
    private final HashingStrategy strategy;
    private final ClusterProxy clusterProxy;

    public Nst1610KVService(int port) throws IOException {
        this(port, List.of(getEndpoint(port)), getEndpoint(port));
    }

    public Nst1610KVService(int port, List<String> clusterEndpoints, String localEndpoint) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = new FileDao(Path.of("storage", Integer.toString(port)));
        this.localEndpoint = localEndpoint;
        this.strategy = new RendezvousHashingStrategy();
        this.strategy.updateEndpoints(clusterEndpoints);
        this.clusterProxy = new ClusterProxy();
        initServer();
    }

    private void initServer() {
        server.createContext("/v0/status", new StatusHandler());
        server.createContext("/v0/entity", new EntityHandler(dao, localEndpoint, strategy, clusterProxy));
    }

    @Override
    public void start() {
        log.info("Server start");
        server.start();
    }

    @Override
    public void stop() {
        log.info("Server stop");
        server.stop(0);
    }

    private static String getEndpoint(int port) {
        return "http://localhost:" + port;
    }

    public void updateClusterEndpoints(List<String> clusterEndpoints) {
        strategy.updateEndpoints(clusterEndpoints);
    }
}
