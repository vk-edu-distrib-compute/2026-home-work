package company.vk.edu.distrib.compute;

import company.vk.edu.distrib.compute.che1nov.KVClusterFactoryImpl;
import company.vk.edu.distrib.compute.che1nov.KVServiceFactoryImpl;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public final class Server {
    private static final String MODE_CLUSTER = "cluster";
    private static final String ALGO_CONSISTENT = "consistent";
    private static final String ALGO_RENDEZVOUS = "rendezvous";
    private static final String REPLICATION_PREFIX_SHORT = "n=";
    private static final String REPLICATION_PREFIX_LONG = "replicas=";

    private Server() {
    }

    public static void main(String[] args) throws IOException {
        var log = LoggerFactory.getLogger("server");
        if (isClusterMode(args)) {
            String shardingAlgorithm = resolveAlgorithm(args);
            int replicationFactor = resolveReplicationFactor(args);
            List<Integer> ports = List.of(8080, 8081);
            KVCluster cluster = new KVClusterFactoryImpl(shardingAlgorithm, replicationFactor).create(ports);
            cluster.start();
            log.info("Cluster started on ports={} algorithm={} replicationFactor={}",
                    ports,
                    shardingAlgorithm,
                    replicationFactor
            );
            Runtime.getRuntime().addShutdownHook(new Thread(cluster::stop));
            return;
        }

        int port = 8080;
        KVService storage = new KVServiceFactoryImpl().create(port);
        storage.start();
        log.info("Server started on port {}", port);
        Runtime.getRuntime().addShutdownHook(new Thread(storage::stop));
    }

    private static boolean isClusterMode(String... args) {
        return Arrays.stream(args).anyMatch(MODE_CLUSTER::equalsIgnoreCase);
    }

    private static String resolveAlgorithm(String... args) {
        for (String arg : args) {
            if (ALGO_CONSISTENT.equalsIgnoreCase(arg) || ALGO_RENDEZVOUS.equalsIgnoreCase(arg)) {
                return arg;
            }
        }
        return KVClusterFactoryImpl.ALGORITHM_RENDEZVOUS;
    }

    private static int resolveReplicationFactor(String... args) {
        for (String arg : args) {
            if (arg.regionMatches(true, 0, REPLICATION_PREFIX_SHORT, 0, REPLICATION_PREFIX_SHORT.length())) {
                return Integer.parseInt(arg.substring(REPLICATION_PREFIX_SHORT.length()));
            }
            if (arg.regionMatches(true, 0, REPLICATION_PREFIX_LONG, 0, REPLICATION_PREFIX_LONG.length())) {
                return Integer.parseInt(arg.substring(REPLICATION_PREFIX_LONG.length()));
            }
        }
        return Integer.parseInt(System.getProperty(KVClusterFactoryImpl.REPLICATION_FACTOR_PROPERTY, "1"));
    }
}
