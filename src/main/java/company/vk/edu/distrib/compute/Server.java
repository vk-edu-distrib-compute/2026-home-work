package company.vk.edu.distrib.compute;

import company.vk.edu.distrib.compute.arslan05t.MyKVServiceFactory;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class Server {
    private static final Logger LOG = Logger.getLogger(Server.class.getName());

    private Server() {
        // Utility class
    }

    public static void main(String[] args) throws IOException {
        int port = 8080;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        }

        KVServiceFactory factory = new MyKVServiceFactory();
        KVService service = factory.create(port);
        service.start();

        if (LOG.isLoggable(Level.INFO)) {
            LOG.info("Server started on port " + port);
        }
    }
}
