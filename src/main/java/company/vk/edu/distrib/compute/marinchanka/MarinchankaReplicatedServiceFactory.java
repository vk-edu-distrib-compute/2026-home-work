package company.vk.edu.distrib.compute.marinchanka;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MarinchankaReplicatedServiceFactory extends KVServiceFactory {
    private static final int DEFAULT_REPLICAS = 3;

    @Override
    protected KVService doCreate(int port) throws IOException {
        int numberOfReplicas = Integer.parseInt(
                System.getProperty("replication.factor", String.valueOf(DEFAULT_REPLICAS)));

        List<Dao<byte[]>> replicas = new ArrayList<>();
        for (int i = 0; i < numberOfReplicas; i++) {
            replicas.add(new VersionedInMemoryDao());
        }

        return new MarinchankaReplicatedService(port, numberOfReplicas, replicas);
    }
}
