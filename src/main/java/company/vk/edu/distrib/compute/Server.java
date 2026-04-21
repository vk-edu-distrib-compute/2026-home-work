package company.vk.edu.distrib.compute;

import company.vk.edu.distrib.compute.usl.UslKVClusterFactory;
import company.vk.edu.distrib.compute.usl.UslKVServiceFactory;
import company.vk.edu.distrib.compute.usl.sharding.ShardingAlgorithm;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public final class Server {
    private Server() {
    }

    public static void main(String... args) throws IOException {
        var log = LoggerFactory.getLogger("server");
        Server server = new Server();
        if (server.isClusterMode(args)) {
            List<Integer> ports = Arrays.asList(8080, 8081);
            KVCluster cluster = new UslKVClusterFactory(server.resolveClusterAlgorithm(args)).create(ports);
            cluster.start();
            log.info("Cluster started on ports={}", ports);
            Runtime.getRuntime().addShutdownHook(new Thread(cluster::stop));
        } else {
            var port = 8080;
            KVService storage = new UslKVServiceFactory().create(port);
            storage.start();
            log.info("Server started on port {}", port);
            Runtime.getRuntime().addShutdownHook(new Thread(storage::stop));
        }
    }

    private boolean isClusterMode(String... args) {
        return args.length > 0 && args[0].startsWith("cluster");
    }

    private ShardingAlgorithm resolveClusterAlgorithm(String... args) {
        return args.length > 1
            ? ShardingAlgorithm.fromExternalName(args[1])
            : ShardingAlgorithm.RENDEZVOUS;
    }
}
