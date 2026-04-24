package company.vk.edu.distrib.compute.artsobol.factory;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.artsobol.impl.ReplicatedKVServiceImpl;

import java.io.IOException;
import java.nio.file.Path;

public class ConcreteKVServiceFactory extends KVServiceFactory {
    private static final String REPLICATION_FACTOR_PROPERTY = "replication.factor";
    private static final int DEFAULT_REPLICATION_FACTOR = 3;

    @Override
    protected KVService doCreate(int port) throws IOException {
        return new ReplicatedKVServiceImpl(
                port,
                resolveReplicationFactor(),
                Path.of("storage", "replication-port-" + port)
        );
    }

    private static int resolveReplicationFactor() {
        String configuredFactor = System.getProperty(REPLICATION_FACTOR_PROPERTY);
        if (configuredFactor == null || configuredFactor.isBlank()) {
            return DEFAULT_REPLICATION_FACTOR;
        }
        return Integer.parseInt(configuredFactor);
    }
}
