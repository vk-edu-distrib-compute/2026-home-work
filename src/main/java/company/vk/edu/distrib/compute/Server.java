package company.vk.edu.distrib.compute;

import company.vk.edu.distrib.compute.golubtsov_pavel.PGClusterFactory;
import company.vk.edu.distrib.compute.golubtsov_pavel.PGReplicatedServiceFactory;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Server {

    public static void main(String... args) throws IOException {
        var log = LoggerFactory.getLogger("server");
        if (isClusterMode(args)) {
            List<Integer> ports = Arrays.asList(8080, 8081);
            KVCluster cluster = new PGClusterFactory().create(ports);
            cluster.start();
            log.info("Cluster started on ports={}", ports);
            Runtime.getRuntime().addShutdownHook(new Thread(cluster::stop));
        } else {
            int port = 8080;
            KVService storage = new PGReplicatedServiceFactory().create(port);
            storage.start();
            log.info("Server started on port {}", port);
            Runtime.getRuntime().addShutdownHook(new Thread(storage::stop));
        }
    }

    private static boolean isClusterMode(String... args) {
        return args.length > 0 && args[0].startsWith("cluster");
    }
}
