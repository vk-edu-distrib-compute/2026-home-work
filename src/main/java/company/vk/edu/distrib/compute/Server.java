package company.vk.edu.distrib.compute;

import module java.base;
import company.vk.edu.distrib.compute.dummy.DummyKVServiceFactory;
import company.vk.edu.distrib.compute.maryarta.KVClusterFactoryImpl;
import company.vk.edu.distrib.compute.maryarta.KVServiceFactoryImpl;
import org.slf4j.LoggerFactory;

public class Server {
//    void main() throws IOException {
//        var log = LoggerFactory.getLogger("server");
//        var port = 8080;
//        KVService storage = new KVServiceFactoryImpl().create(port);
//        storage.start();
//        log.info("Server started on port {}", port);
//        Runtime.getRuntime().addShutdownHook(new Thread(storage::stop));
//    }
    void main(String... args) throws IOException {
        var log = LoggerFactory.getLogger("server");
        if (isClusterMode(args)) {
            List<Integer> ports = Arrays.asList(8080, 8081, 8082, 8083, 8084);
            KVCluster cluster = new KVClusterFactoryImpl().create(ports, replicationFactor(args));
            cluster.start();
            log.info("Cluster started on ports={}", ports);
            Runtime.getRuntime().addShutdownHook(new Thread(cluster::stop));
        } else {
            var port = 8080;
            KVService storage = new KVServiceFactoryImpl().create(port);
            storage.start();
            log.info("Server started on port {}", port);
            Runtime.getRuntime().addShutdownHook(new Thread(storage::stop));
        }
    }

    private boolean isClusterMode(String... args) {
        return args.length > 0 && args[0].startsWith("cluster");
    }

    private int replicationFactor(String... args) {
        for (String arg : args) {
            if (arg.startsWith("--replication-factor=")) {
                int replicationFactor = Integer.parseInt(arg.substring("--replication-factor=".length()));
                return replicationFactor > 0 ? replicationFactor : 1;
            }
        }
        return 1;
    }

}
