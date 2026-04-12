package company.vk.edu.distrib.compute;

import module java.base;
//import company.vk.edu.distrib.compute.dummy.DummyKVServiceFactory;
import company.vk.edu.distrib.compute.semenmartynov.SemenMartynovKVServiceFactory;
import org.slf4j.LoggerFactory;

public class Server {

    void main() throws IOException {
        var log = LoggerFactory.getLogger("server");
        var port = 8080;
        //KVService storage = new DummyKVServiceFactory().create(port);
        KVService storage = new SemenMartynovKVServiceFactory().create(port);
        storage.start();
        log.info("Server started on port {}", port);
        Runtime.getRuntime().addShutdownHook(new Thread(storage::stop));
    }
}
