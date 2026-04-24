package company.vk.edu.distrib.compute.luckyslon2003;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class LuckySlon2003KVServiceFactory extends KVServiceFactory {
    private static final int MIN_REPLICA_COUNT = 1;
    private static final String REPLICA_COUNT_PROPERTY = "luckyslon2003.replication.factor";
    private static final int DEFAULT_REPLICA_COUNT = 3;

    @Override
    protected KVService doCreate(int port) throws IOException {
        int replicaCount = Integer.getInteger(REPLICA_COUNT_PROPERTY, DEFAULT_REPLICA_COUNT);
        if (replicaCount < MIN_REPLICA_COUNT) {
            throw new IllegalArgumentException("Replica count must be positive");
        }

        List<Dao<byte[]>> replicaDaos = new ArrayList<>(replicaCount);
        Path serviceDirectory = dataDirectory().resolve("port-" + port);
        for (int replicaId = 0; replicaId < replicaCount; replicaId++) {
            replicaDaos.add(new FileDao(serviceDirectory.resolve("replica-" + replicaId)));
        }
        return new LuckySlon2003ReplicatedService(port, List.copyOf(replicaDaos));
    }

    protected Path dataDirectory() {
        return Path.of(System.getProperty("user.dir"), "build", "luckyslon2003-data");
    }
}
