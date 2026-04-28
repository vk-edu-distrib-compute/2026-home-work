package company.vk.edu.distrib.compute.wedwincode.replicated;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.wedwincode.PersistentDao;
import company.vk.edu.distrib.compute.wedwincode.DaoRecord;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class EdwinReplicatedKVServiceFactory extends KVServiceFactory {
    private static final int DEFAULT_REPLICATION_FACTOR = 3;
    private static final Path DATA_DIR = Path.of(".data", "wedwincode", "replicated");

    @Override
    protected KVService doCreate(int port) throws IOException {
        int replicasCount = getReplicationFactor();
        return new ReplicatedKVServiceImpl(port, buildPersistentDaoList(port, replicasCount));
    }

    private List<Dao<DaoRecord>> buildPersistentDaoList(int port, int replicasCount) throws IOException {
        List<Dao<DaoRecord>> daoList = new ArrayList<>();
        for (int i = 0; i < replicasCount; i++) {
            Path replicaPath = DATA_DIR.resolve("port_" + port, "replica_" + i);
            Files.createDirectories(replicaPath);
            daoList.add(new PersistentDao(replicaPath));
        }

        return daoList;
    }

    private static int getReplicationFactor() {
        String raw = System.getenv("REPLICATION_FACTOR");
        if (raw == null) {
            return DEFAULT_REPLICATION_FACTOR;
        }

        try {
            int factor = Integer.parseInt(raw);
            if (factor > 0) {
                return factor;
            }
            throw new IllegalArgumentException("factor must be greater than 0");
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("env variable is invalid", e);
        }
    }
}
