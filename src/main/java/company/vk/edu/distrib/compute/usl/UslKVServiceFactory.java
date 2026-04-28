package company.vk.edu.distrib.compute.usl;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.io.IOException;

public class UslKVServiceFactory extends KVServiceFactory {
    public static final String REPLICATION_FACTOR_PROPERTY =
        "company.vk.edu.distrib.compute.usl.replication.factor";
    public static final String REPLICATION_FACTOR_ENV =
        "COMPANY_VK_EDU_DISTRIB_COMPUTE_USL_REPLICATION_FACTOR";
    private static final int DEFAULT_REPLICATION_FACTOR = 3;
    private static final int MIN_REPLICATION_FACTOR = 3;

    @Override
    protected KVService doCreate(int port) throws IOException {
        Path storageRoot = Files.createTempDirectory("usl-replicated-service-" + port + '-');
        return new UslReplicatedKVService(port, resolveReplicationFactor(), storageRoot);
    }

    private static int resolveReplicationFactor() {
        String propertyValue = System.getProperty(REPLICATION_FACTOR_PROPERTY);
        if (propertyValue != null && !propertyValue.isBlank()) {
            return parseReplicationFactor(propertyValue);
        }

        String envValue = System.getenv(REPLICATION_FACTOR_ENV);
        if (envValue != null && !envValue.isBlank()) {
            return parseReplicationFactor(envValue);
        }

        return DEFAULT_REPLICATION_FACTOR;
    }

    private static int parseReplicationFactor(String rawValue) {
        int replicationFactor = Integer.parseInt(rawValue.trim());
        if (replicationFactor < MIN_REPLICATION_FACTOR) {
            throw new IllegalArgumentException(
                "Replication factor must be at least " + MIN_REPLICATION_FACTOR
            );
        }
        return replicationFactor;
    }
}
