package company.vk.edu.distrib.compute.bushuev_a_s;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;

public class MyKVServiceFactory extends KVServiceFactory {
    private static final Logger log = LoggerFactory.getLogger(MyKVServiceFactory.class);
    private MyKVCluster cluster;

    public MyKVServiceFactory() {
        super();
    }

    public MyKVServiceFactory(MyKVCluster cluster) {
        super();
        this.cluster = cluster;
    }

    @Override
    protected KVService doCreate(int port) throws IOException {
        log.debug("Creating KVService on port {}", port);
        return new MyKVService(port, new MyFileDao(Path.of("data")), cluster);
    }
}
