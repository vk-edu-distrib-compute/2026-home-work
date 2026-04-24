package company.vk.edu.distrib.compute.lillymega;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;

import java.io.IOException;

public class LillymegaKVServiceFactory extends KVServiceFactory {
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
            return 3;
        }

        int parsedValue = Integer.parseInt(configuredValue);
        if (parsedValue < 1) {
            throw new IllegalArgumentException("Replication factor must be positive");
        }
        return parsedValue;
    }
}
