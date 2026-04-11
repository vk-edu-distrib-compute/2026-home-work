package company.vk.edu.distrib.compute;

import company.vk.edu.distrib.compute.artsobol.ConcreteKVServiceFactory;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Server {

    void main() throws IOException {
        var log = LoggerFactory.getLogger("server");
        var port = 8080;
        KVService storage = new ConcreteKVServiceFactory().create(port);
        storage.start();
        log.info("Server started on port {}", port);
        Runtime.getRuntime().addShutdownHook(new Thread(storage::stop));
    }
}
