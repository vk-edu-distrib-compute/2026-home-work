package company.vk.edu.distrib.compute.martinez1337.service;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.martinez1337.dao.FileDao;
import company.vk.edu.distrib.compute.martinez1337.sharding.RendezvousSharding;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class Martinez1337KVServiceFactory extends KVServiceFactory {
    private static final String REPLICATION_FACTOR_ENV = "MARTINEZ1337_REPLICATION_FACTOR";
    private static final int DEFAULT_REPLICATION_FACTOR = 3;

    @Override
    protected KVService doCreate(int port) throws IOException {
        int replicationFactor = resolveReplicationFactor();
        Path baseDir = Path.of("martinez1337-storage", "martinez1337-replicas", String.valueOf(port));

        List<Dao<byte[]>> replicas = new ArrayList<>(replicationFactor);
        for (int i = 0; i < replicationFactor; i++) {
            replicas.add(new FileDao(baseDir.resolve("replica-" + i)));
        }

        return new Martinez1337KVService(port, replicas, new RendezvousSharding());
    }

    private int resolveReplicationFactor() {
        String configuredValue = System.getenv(REPLICATION_FACTOR_ENV);
        if (configuredValue == null || configuredValue.isBlank()) {
            return DEFAULT_REPLICATION_FACTOR;
        }
        try {
            int factor = Integer.parseInt(configuredValue);
            if (factor <= 0) {
                throw new IllegalArgumentException("Replication factor must be positive");
            }
            return factor;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid replication factor: " + configuredValue, e);
        }
    }
}
