package company.vk.edu.distrib.compute.tadzhnahal;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class TadzhnahalKVServiceFactory extends KVServiceFactory {
    private final TadzhnahalShardingAlgorithm shardingAlgorithm;

    public TadzhnahalKVServiceFactory() {
        super();
        this.shardingAlgorithm = TadzhnahalShardingAlgorithm.RENDEZVOUS;
    }

    public TadzhnahalKVServiceFactory(TadzhnahalShardingAlgorithm shardingAlgorithm) {
        super();

        if (shardingAlgorithm == null) {
            throw new IllegalArgumentException("Sharding algorithm must not be null");
        }

        this.shardingAlgorithm = shardingAlgorithm;
    }

    @Override
    protected KVService doCreate(int port) throws IOException {
        FileDao dao = new FileDao(storageDir(port));
        return new TadzhnahalKVService(
                port,
                dao,
                List.of(endpoint(port)),
                shardingAlgorithm
        );
    }

    public KVService create(int port, List<String> clusterEndpoints) throws IOException {
        if (clusterEndpoints == null || clusterEndpoints.isEmpty()) {
            throw new IllegalArgumentException("Cluster endpoints must not be empty");
        }

        FileDao dao = new FileDao(storageDir(port));
        return new TadzhnahalKVService(
                port,
                dao,
                clusterEndpoints,
                shardingAlgorithm
        );
    }

    private static Path storageDir(int port) {
        return Path.of(
                System.getProperty("user.home"),
                ".vk-distributed-computing",
                "tadzhnahal",
                "port-" + port
        );
    }

    private static String endpoint(int port) {
        return "http://localhost:" + port;
    }
}
