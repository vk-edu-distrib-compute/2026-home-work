package company.vk.edu.distrib.compute.martinez1337.service;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.martinez1337.controller.EntityHttpHandler;
import company.vk.edu.distrib.compute.martinez1337.controller.StatusHandler;
import company.vk.edu.distrib.compute.martinez1337.sharding.RendezvousSharding;
import company.vk.edu.distrib.compute.martinez1337.sharding.ShardingStrategy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

public class Martinez1337KVService implements KVService, ClusterAwareKVService {
    private final Dao<byte[]> dao;
    private final HttpServer server;

    private List<String> clusterEndpoints = List.of();
    private int myNodeId = -1;
    private final ShardingStrategy sharding;

    private StatusHandler statusHandler;
    private EntityHttpHandler entityHandler;

    public Martinez1337KVService(int port, Dao<byte[]> dao) throws IOException {
        this(port, dao, new RendezvousSharding());
    }

    public Martinez1337KVService(int port, Dao<byte[]> dao, ShardingStrategy sharding) throws IOException {
        this.dao = dao;
        this.sharding = sharding;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        initServer();
    }

    private void initServer() {
        statusHandler = new StatusHandler(clusterEndpoints, sharding);
        entityHandler = new EntityHttpHandler(dao, clusterEndpoints, sharding);
        server.createContext("/v0/status", statusHandler);
        server.createContext("/v0/entity", entityHandler);
    }

    @Override
    public void start() {
        this.server.start();
    }

    @Override
    public void stop() {
        this.server.stop(1);
    }

    @Override
    public void setCluster(List<String> allEndpoints, int myNodeId) {
        this.clusterEndpoints = List.copyOf(allEndpoints);
        this.myNodeId = myNodeId;

        if (statusHandler != null) {
            statusHandler.setClusterInfo(this.clusterEndpoints, this.myNodeId);
        }
        if (entityHandler != null) {
            entityHandler.setClusterInfo(this.clusterEndpoints, this.myNodeId);
        }
    }
}
