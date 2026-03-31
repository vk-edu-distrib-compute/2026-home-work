package company.vk.edu.distrib.compute;

import module java.base;
import org.slf4j.LoggerFactory;
import company.vk.edu.distrib.compute.kuznetsovasvetlana6.MyKVServiceFactoryFile;

public class Server {
    void main() throws IOException {
        var log = LoggerFactory.getLogger("server");
        var port = 8080;
        KVService storage = new MyKVServiceFactoryFile().create(port);
        storage.start();
        log.info("Server started on port {}", port);
        Runtime.getRuntime().addShutdownHook(new Thread(storage::stop));
    }
}
