package company.vk.edu.distrib.compute.martinez1337.service;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.ReplicatedService;
import company.vk.edu.distrib.compute.martinez1337.controller.EntityHttpHandler;
import company.vk.edu.distrib.compute.martinez1337.controller.StatusHandler;
import company.vk.edu.distrib.compute.martinez1337.replication.ReplicationManager;
import company.vk.edu.distrib.compute.martinez1337.sharding.RendezvousSharding;
import company.vk.edu.distrib.compute.martinez1337.sharding.ShardingStrategy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Martinez1337KVService implements ClusterAwareKVService, ReplicatedService {
    private static final Logger log = LoggerFactory.getLogger(Martinez1337KVService.class);

    private final int port;
    private final HttpServer server;
    private final ReplicationManager replicationManager;

    private List<String> clusterEndpoints = List.of();
    private final ShardingStrategy sharding;

    private StatusHandler statusHandler;
    private EntityHttpHandler entityHandler;

    public Martinez1337KVService(int port, Dao<byte[]> dao) throws IOException {
        this(port, dao, new RendezvousSharding());
    }

    public Martinez1337KVService(
            int port,
            Dao<byte[]> dao,
            ShardingStrategy sharding
    ) throws IOException {
        this(port, List.of(dao), sharding);
    }

    public Martinez1337KVService(
            int port,
            List<Dao<byte[]>> replicaDaos,
            ShardingStrategy sharding
    ) throws IOException {
        this.port = port;
        this.sharding = sharding;
        this.replicationManager = new ReplicationManager(replicaDaos, sharding);
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        initServer();
    }

    private void initServer() {
        statusHandler = new StatusHandler(clusterEndpoints, sharding);
        entityHandler = new EntityHttpHandler(clusterEndpoints, sharding, replicationManager);
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
        try {
            this.replicationManager.close();
        } catch (IOException e) {
            log.warn("Failed to close replica daos", e);
        }
    }

    @Override
    public void setCluster(List<String> allEndpoints, int myNodeId) {
        this.clusterEndpoints = List.copyOf(allEndpoints);

        if (statusHandler != null) {
            statusHandler.setClusterInfo(this.clusterEndpoints, myNodeId);
        }
        if (entityHandler != null) {
            entityHandler.setClusterInfo(this.clusterEndpoints, myNodeId);
        }
    }

    @Override
    public int port() {
        return port;
    }

    @Override
    public int numberOfReplicas() {
        return replicationManager.replicasCount();
    }

    @Override
    public void disableReplica(int nodeId) {
        replicationManager.disableReplica(nodeId);
    }

    @Override
    public void enableReplica(int nodeId) {
        replicationManager.enableReplica(nodeId);
    }
}
