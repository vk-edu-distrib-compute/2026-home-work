package company.vk.edu.distrib.compute;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public final class Server {
    private static final Logger LOG = LoggerFactory.getLogger(Server.class);
    private static final int PORT = 8000;
    private static final String DEFAULT_STORAGE_DIR = "./data";

    private Server() {
    }

    static void main(String[] args) {
        try {
            Dao<byte[]> dao = new FileSystemDao(DEFAULT_STORAGE_DIR);
            SimpleKVService service = new SimpleKVService(PORT, dao);
            service.start();
            LOG.info("KVService started on port {}", PORT);
            LOG.info("Press Enter to stop...");
            System.in.read();
            service.stop();
            LOG.info("KVService stopped");
        } catch (IOException e) {
            LOG.error("IO error while starting service", e);
            System.exit(1);
        } catch (Exception e) {
            LOG.error("Ошибка: " + e.getMessage());
            System.exit(1);
        }
    }
}
