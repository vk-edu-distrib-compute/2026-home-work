package company.vk.edu.distrib.compute.vladislavGuzov;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

public class InMemoryKVService implements KVService {
    private static final Logger log = LoggerFactory.getLogger(InMemoryKVService.class);

    private final HttpServer server;

    public InMemoryKVService(int port) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);

    }

    @Override
    public void start() {
        log.info("Starting");
        server.start();
    }

    @Override
    public void stop() {
        log.info("Stopped");
        server.stop(1);
    }
}
