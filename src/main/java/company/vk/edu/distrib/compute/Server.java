package company.vk.edu.distrib.compute;

import module java.base;
import company.vk.edu.distrib.compute.vodobryshkin.DefaultKVServiceFactory;
import company.vk.edu.distrib.compute.vodobryshkin.FileDao;
import org.slf4j.LoggerFactory;

public class Server {

    void main() throws IOException {
        var log = LoggerFactory.getLogger("server");
        var port = 8080;
        Dao<byte[]> dao = new FileDao();
        KVService storage = new DefaultKVServiceFactory(dao).create(port);
        storage.start();
        log.info("Server started on port {}", port);
        Runtime.getRuntime().addShutdownHook(new Thread(storage::stop));
    }
}
