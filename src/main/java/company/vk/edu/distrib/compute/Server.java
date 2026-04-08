package company.vk.edu.distrib.compute;

import module java.base;
import company.vk.edu.distrib.compute.andrey1af.service.Andrey1afKVServiceFactory;
import org.slf4j.LoggerFactory;

public class Server {

    void main(String... args) throws IOException {
        var log = LoggerFactory.getLogger("server");
        var port = 8080;
        KVService storage = new Andrey1afKVServiceFactory().create(port);
        storage.start();
        log.info("Server started on port {}", port);
        Runtime.getRuntime().addShutdownHook(new Thread(storage::stop));
    }
}
