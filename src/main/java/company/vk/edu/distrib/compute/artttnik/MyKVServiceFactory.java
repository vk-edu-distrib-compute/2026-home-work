package company.vk.edu.distrib.compute.artttnik;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class MyKVServiceFactory extends KVServiceFactory {
    private static final Logger log = LoggerFactory.getLogger(MyKVServiceFactory.class);

    private final int replicaCount;

    public MyKVServiceFactory() {
        super();
        this.replicaCount = ReplicationConfig.resolveReplicaCount();
    }

    public MyKVServiceFactory(int replicaCount) {
        super();
        this.replicaCount = replicaCount;
    }

    @Override
    protected KVService doCreate(int port) {
        log.debug("Creating KVService on port {}", port);
        return new MyReplicatedKVService(port, createReplicaDaos(port));
    }

    private List<company.vk.edu.distrib.compute.Dao<byte[]>> createReplicaDaos(int port) {
        Path serviceRoot = Path.of("data", "artttnik-single", String.valueOf(port));
        List<company.vk.edu.distrib.compute.Dao<byte[]>> result = new ArrayList<>(replicaCount);

        for (int i = 0; i < replicaCount; i++) {
            result.add(new MyDao(serviceRoot.resolve("replica-" + i)));
        }

        return result;
    }
}
