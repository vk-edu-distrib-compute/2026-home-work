package company.vk.edu.distrib.compute;

import module java.base;
import company.vk.edu.distrib.compute.bushuev_a_s.MyRendezvousHashing;
import company.vk.edu.distrib.compute.bushuev_a_s.MyKVClusterFactory;
import company.vk.edu.distrib.compute.bushuev_a_s.MyKVServiceFactory;
import org.slf4j.LoggerFactory;

public class Server {

    void main(String... args) throws IOException {
        var log = LoggerFactory.getLogger("server");
        if (isClusterMode(args)) {
            List<Integer> ports = Arrays.asList(8080, 8081);
            KVCluster cluster = new MyKVClusterFactory().create(ports, new MyRendezvousHashing());
            cluster.start();
            log.info("Cluster started on ports={}", ports);
            Runtime.getRuntime().addShutdownHook(new Thread(cluster::stop));
        } else {
            var port = 8080;
            KVService storage = new MyKVServiceFactory().create(port);
            storage.start();
            log.info("Server started on port {}", port);
            Runtime.getRuntime().addShutdownHook(new Thread(storage::stop));
        }
    }

    private boolean isClusterMode(String... args) {
        return args.length > 0 && args[0].startsWith("cluster");
    }
}
