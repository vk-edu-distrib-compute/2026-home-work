package company.vk.edu.distrib.compute;

import module java.base;
import company.vk.edu.distrib.compute.vladislavguzov.ClusterPorts;
import company.vk.edu.distrib.compute.vladislavguzov.MyKVServiceFactory;
import company.vk.edu.distrib.compute.vladislavguzov.MyKVClusterFactoty;
import org.slf4j.LoggerFactory;

public class Server {

    void main(String... args) throws IOException {
        var log = LoggerFactory.getLogger("server");
        if (isClusterMode(args)) {
            List<Integer> ports;
            if (args.length >= 2) {
                ports = new ClusterPorts(args[1]).getPortsList();
            } else {
                ports = Arrays.asList(8080, 8081);
            }
            KVCluster cluster = new MyKVClusterFactoty().create(ports);
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
