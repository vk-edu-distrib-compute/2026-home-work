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

    private Server() {
    }

    public static void main(String[] args) throws IOException {
        var log = LoggerFactory.getLogger("server");
        if (isClusterMode(args)) {
            String shardingAlgorithm = resolveAlgorithm(args);
            List<Integer> ports = List.of(8080, 8081);
            KVCluster cluster = new KVClusterFactoryImpl(shardingAlgorithm).create(ports);
            cluster.start();
            log.info("Cluster started on ports={} algorithm={}", ports, shardingAlgorithm);
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
}
