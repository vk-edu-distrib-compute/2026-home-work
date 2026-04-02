package company.vk.edu.distrib.compute.bobridze5;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.bobridze5.handlers.EntityHandler;
import company.vk.edu.distrib.compute.bobridze5.handlers.StatusHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class BobridzeKVService implements KVService {
    private static final Logger log = LoggerFactory.getLogger(BobridzeKVService.class);

    private final HttpServer server;
    private final InetSocketAddress addr;
    private final Dao<byte[]> dao;
    private boolean isStarted;

    public BobridzeKVService(int port, Dao<byte[]> dao) throws IOException {
        this.addr = new InetSocketAddress(InetAddress.getLoopbackAddress(), port);
        this.server = HttpServer.create();
        this.dao = dao;
        initContext();
    }

    private void initContext() {
        server.createContext("/v0/status", new StatusHandler());
        server.createContext("/v0/entity", new EntityHandler(dao));
    }

    @Override
    public void start() {
        if (isStarted) {
            throw new IllegalArgumentException("Server already started");
        }

        try {
            server.setExecutor(Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors()));
            server.bind(addr, 0);
            server.start();
            isStarted = true;
        } catch (IOException e) {
            log.error("");
        }
    }

    @Override
    public void stop() {
        if (!isStarted) {
            throw new IllegalArgumentException("Server is not started");
        }

        server.stop(0);
        isStarted = false;
    }
}
