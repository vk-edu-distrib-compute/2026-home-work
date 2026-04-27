package company.vk.edu.distrib.compute.kruchinina.replication;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.ReplicatedService;
import company.vk.edu.distrib.compute.kruchinina.sharding.ShardingStrategy;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class ReplicatedKVService implements ReplicatedService {

    private static final int MIN_REPLICAS_FOR_DAO = 1;

    private final SimpleKVService service;
    private final Optional<ReplicatedFileSystemDao> replicatedDao;
    private final int port;
    private final int numberOfReplicas;

    public ReplicatedKVService(int port, int replicas) throws IOException {
        this.port = port;
        this.numberOfReplicas = replicas;
        String storagePath = "./data-" + port;
        if (replicas > MIN_REPLICAS_FOR_DAO) {
            ReplicatedFileSystemDao dao = new ReplicatedFileSystemDao(storagePath, replicas);
            this.replicatedDao = Optional.of(dao);
            this.service = new SimpleKVService(port, dao);
        } else {
            this.replicatedDao = Optional.empty();
            this.service = new SimpleKVService(port, new FileSystemDao(storagePath));
        }
    }

    public ReplicatedKVService(int port, Dao<byte[]> dao,
                               List<String> clusterNodes, String selfAddress,
                               ShardingStrategy shardingStrategy) {
        this.port = port;
        if (dao instanceof ReplicatedFileSystemDao) {
            ReplicatedFileSystemDao repDao = (ReplicatedFileSystemDao) dao;
            this.replicatedDao = Optional.of(repDao);
            this.numberOfReplicas = repDao.getReplicaCount();
        } else {
            this.replicatedDao = Optional.empty();
            this.numberOfReplicas = 1;
        }
        this.service = new SimpleKVService(port, dao, clusterNodes, selfAddress, shardingStrategy);
    }

    @Override
    public void start() {
        service.start();
    }

    @Override
    public void stop() {
        service.stop();
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
        replicatedDao.ifPresent(dao -> dao.disableReplica(nodeId));
    }

    @Override
    public void enableReplica(int nodeId) {
        replicatedDao.ifPresent(dao -> dao.enableReplica(nodeId));
    }
}
