package company.vk.edu.distrib.compute;

import company.vk.edu.distrib.compute.tadzhnahal.TadzhnahalKVClusterFactory;
import company.vk.edu.distrib.compute.tadzhnahal.TadzhnahalKVServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Server {
    private static final Logger log = LoggerFactory.getLogger(Server.class);

    void main(String... args) throws IOException {
        if (isClusterMode(args)) {
            List<Integer> ports = Arrays.asList(8080, 8081);
            KVCluster cluster = new TadzhnahalKVClusterFactory().create(ports);
            cluster.start();
            log.info("Cluster started on ports={}", ports);
            Runtime.getRuntime().addShutdownHook(new Thread(cluster::stop));
            return;
        }

        int port = 8080;
        KVService storage = new TadzhnahalKVServiceFactory().create(port);
        storage.start();
        log.info("Server started on port {}", port);
        Runtime.getRuntime().addShutdownHook(new Thread(storage::stop));
    }

    private boolean isClusterMode(String... args) {
        return args.length > 0 && args[0].startsWith("cluster");
    }
}
