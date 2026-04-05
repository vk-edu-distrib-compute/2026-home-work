package company.vk.edu.distrib.compute.BahadirAhmedov;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Objects;


public class BahadirAhmedovKVService implements KVService {
    private final Dao<byte[]> storage;
    private static final String GET = "GET";

    private static final Logger log = LoggerFactory.getLogger(BahadirAhmedovKVService.class);

    private final HttpServer server;

    public BahadirAhmedovKVService(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        storage = new InMemoryDao();

        initServer();
    }

    private void initServer() {
        if (server == null) {
            log.error("Server is null");
        }else {
            server.createContext("/v0/status", this::handleStatus);
        }
    }

    @Override
    public void start() {
        server.start();
        log.info("Started");
    }

    @Override
    public void stop() {
        server.stop(0);
        log.info("Stopping");
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        String requestMethod = exchange.getRequestMethod();
        if (!Objects.equals(requestMethod, GET)) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }
        exchange.sendResponseHeaders(200, 0);
        exchange.close();
    }

}