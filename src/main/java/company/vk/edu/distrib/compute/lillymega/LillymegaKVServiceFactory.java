package company.vk.edu.distrib.compute.lillymega;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

public class LillymegaKVServiceFactory extends KVServiceFactory {
    private static final int DEFAULT_REPLICATION_FACTOR = 3;
    private static final int MIN_REPLICATION_FACTOR = 1;

    @Override
    protected KVService doCreate(int port) throws IOException {
        return new LillymegaReplicatedService(port, resolveReplicationFactor());
    }

    private int resolveReplicationFactor() {
        String configuredValue = System.getProperty("lillymega.replication.factor");
        if (configuredValue == null || configuredValue.isBlank()) {
            configuredValue = System.getenv("LILLYMEGA_REPLICATION_FACTOR");
        }
        if (configuredValue == null || configuredValue.isBlank()) {
            return DEFAULT_REPLICATION_FACTOR;
        }

        int parsedValue = Integer.parseInt(configuredValue);
        if (parsedValue < MIN_REPLICATION_FACTOR) {
            throw new IllegalArgumentException("Replication factor must be positive");
        }
        return parsedValue;
    }
}
