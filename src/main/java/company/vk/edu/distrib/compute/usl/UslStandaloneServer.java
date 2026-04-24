package company.vk.edu.distrib.compute.usl;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;

public final class UslStandaloneServer {
    private static final int DEFAULT_REPLICATION_FACTOR = 3;

    private UslStandaloneServer() {
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = args.length > 0 ? Integer.parseInt(args[0]) : 8080;
        Path dataDirectory = args.length > 1
            ? Path.of(args[1])
            : StoragePaths.persistentDataDir(port);
        int replicationFactor = args.length > 2
            ? Integer.parseInt(args[2])
            : resolveReplicationFactor();

        UslReplicatedKVService service = new UslReplicatedKVService(
            port,
            replicationFactor,
            dataDirectory.resolve("replicated")
        );
        service.start();
        Runtime.getRuntime().addShutdownHook(new Thread(service::stop));
        new CountDownLatch(1).await();
    }

    private static int resolveReplicationFactor() {
        String propertyValue = System.getProperty(UslKVServiceFactory.REPLICATION_FACTOR_PROPERTY);
        if (propertyValue != null && !propertyValue.isBlank()) {
            return Integer.parseInt(propertyValue.trim());
        }

        String envValue = System.getenv(UslKVServiceFactory.REPLICATION_FACTOR_ENV);
        if (envValue != null && !envValue.isBlank()) {
            return Integer.parseInt(envValue.trim());
        }

        return DEFAULT_REPLICATION_FACTOR;
    }
}
