package company.vk.edu.distrib.compute;

//import module java.base;
import java.io.IOException;

import company.vk.edu.distrib.compute.BahadirAhmedov.BahadirAhmedovKVServiceFactory;
import company.vk.edu.distrib.compute.dummy.DummyKVServiceFactory;
import org.slf4j.LoggerFactory;

public class Server {

    public static void main(String[] args) throws IOException {
        var log = LoggerFactory.getLogger("server");
        var port = 8080;
        KVService storage = new BahadirAhmedovKVServiceFactory().create(port);
        storage.start();
        log.info("Server started on port {}", port);
//        Runtime.getRuntime().addShutdownHook(new Thread(storage::stop));
    }
}
