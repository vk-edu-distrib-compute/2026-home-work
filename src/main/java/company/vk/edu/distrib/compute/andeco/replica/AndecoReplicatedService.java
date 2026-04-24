package company.vk.edu.distrib.compute.andeco.replica;

import company.vk.edu.distrib.compute.ReplicatedService;
import company.vk.edu.distrib.compute.andeco.controller.StatsController;
import company.vk.edu.distrib.compute.andeco.controller.StatusController;
import company.vk.edu.distrib.compute.andeco.sharding.AndecoShardingKVServiceImpl;
import company.vk.edu.distrib.compute.andeco.sharding.ShardingStrategy;

import java.io.IOException;

public class AndecoReplicatedService extends AndecoShardingKVServiceImpl implements ReplicatedService {
    private final int port;
    private final int numberOfReplicas;
    private final ReplicaEntityController controller;
    private final StatsController statsController;

    public AndecoReplicatedService(int port, ShardingStrategy<String> strategy, int numberOfReplicas)
            throws IOException {
        this(port, strategy, numberOfReplicas, new ReplicaEntityController(port, numberOfReplicas));
    }

    private AndecoReplicatedService(int port,
                                    ShardingStrategy<String> strategy,
                                    int numberOfReplicas,
                                    ReplicaEntityController controller) throws IOException {
        super(port, strategy, controller, new StatusController());
        this.port = port;
        this.numberOfReplicas = numberOfReplicas;
        this.controller = controller;
        this.statsController = new StatsController(controller.getReplicas());

        server.createContext("/v0/stats", statsController::processRequest);
    }

    @Override
    public int port() {
        return port;
    }

    @Override
    public int numberOfReplicas() {
        return numberOfReplicas;
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
