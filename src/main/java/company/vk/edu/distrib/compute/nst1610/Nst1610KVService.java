package company.vk.edu.distrib.compute.nst1610;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.nst1610.dao.InMemoryDao;
import company.vk.edu.distrib.compute.nst1610.http.EntityHandler;
import company.vk.edu.distrib.compute.nst1610.http.StatusHandler;
import java.io.IOException;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Nst1610KVService implements KVService {
    private static final Logger log = LoggerFactory.getLogger(Nst1610KVService.class);
    private final HttpServer server;
    private final Dao<byte[]> dao;

    public Nst1610KVService(int port) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.dao = new InMemoryDao();
        initServer();
    }

    private void initServer() {
        server.createContext("/v0/status", new StatusHandler());
        server.createContext("/v0/entity", new EntityHandler(dao));
    }

    @Override
    public void start() {
        log.info("Server start");
        server.start();
    }

    @Override
    public void stop() {
        log.info("Server stop");
        server.stop(0);
    }
}
