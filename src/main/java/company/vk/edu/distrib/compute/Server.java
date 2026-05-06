package company.vk.edu.distrib.compute;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import company.vk.edu.distrib.compute.goshanchic.KVCluster;
import company.vk.edu.distrib.compute.goshanchic.KVClusterFactoryImpl;
import company.vk.edu.distrib.compute.goshanchic.KVServiceFactoryImpl;
import org.slf4j.LoggerFactory;

public class Server {

    public void main(String... args) throws IOException {
        var log = LoggerFactory.getLogger("server");
        if (isClusterMode(args)) {
            List<Integer> ports = Arrays.asList(8080, 8081);
            KVCluster cluster = new KVClusterFactoryImpl().doCreate(ports);
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
}
