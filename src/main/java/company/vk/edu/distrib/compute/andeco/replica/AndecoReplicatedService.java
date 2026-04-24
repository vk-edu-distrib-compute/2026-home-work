package company.vk.edu.distrib.compute.andeco.replica;

import company.vk.edu.distrib.compute.ReplicatedService;
import company.vk.edu.distrib.compute.andeco.controller.StatsController;
import company.vk.edu.distrib.compute.andeco.controller.StatusController;
import company.vk.edu.distrib.compute.andeco.sharding.AndecoShardingKVServiceImpl;
import company.vk.edu.distrib.compute.andeco.sharding.ShardingStrategy;

import java.io.IOException;

public class AndecoReplicatedService extends AndecoShardingKVServiceImpl implements ReplicatedService {
    private final int replicas;
    private final ReplicaEntityController controller;

    public AndecoReplicatedService(int port, ShardingStrategy<String> strategy, int replicas)
            throws IOException {
        this(port, strategy, replicas, new ReplicaEntityController(port, replicas));
    }

    private AndecoReplicatedService(int port,
                                    ShardingStrategy<String> strategy,
                                    int replicas,
                                    ReplicaEntityController controller) throws IOException {
        super(port, strategy, controller, new StatusController());
        this.currentPort = port;
        this.replicas = replicas;
        this.controller = controller;
        StatsController statsController = new StatsController(controller.getReplicas());

        server.createContext("/v0/stats", statsController::processRequest);
    }

    @Override
    public int port() {
        return currentPort;
    }

    @Override
    public int numberOfReplicas() {
        return replicas;
    }

    @Override
    public void disableReplica(int nodeId) {
        controller.disableReplica(nodeId);
    }

    @Override
    public void enableReplica(int nodeId) {
        controller.enableReplica(nodeId);
    }
}
