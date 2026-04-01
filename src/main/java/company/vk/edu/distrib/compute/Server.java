package company.vk.edu.distrib.compute;

import java.io.IOException;
import company.vk.edu.distrib.compute.d1gitale.KVServiceFactoryImpl;
import org.slf4j.LoggerFactory;

public final class Server {

    private Server() {
        // Utility class
    }

    public static void main(String[] args) throws IOException {
        var log = LoggerFactory.getLogger("server");
        var port = 8080;
        KVService storage = new KVServiceFactoryImpl().create(port);
        storage.start();
        log.info("Server started on port {}", port);
        Runtime.getRuntime().addShutdownHook(new Thread(storage::stop));
    }
}
