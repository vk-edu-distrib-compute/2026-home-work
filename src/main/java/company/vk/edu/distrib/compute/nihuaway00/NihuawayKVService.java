package company.vk.edu.distrib.compute.nihuaway00;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.nihuaway00.http.EntityHandler;
import company.vk.edu.distrib.compute.nihuaway00.http.PingHandler;
import company.vk.edu.distrib.compute.nihuaway00.sharding.ShardRouter;
import company.vk.edu.distrib.compute.nihuaway00.storage.EntityDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.concurrent.Executors;

public class NihuawayKVService implements company.vk.edu.distrib.compute.KVService {
    private static final Logger log = LoggerFactory.getLogger(NihuawayKVService.class);

    private HttpServer server;
    private final ShardRouter shardRouter;
    int port;

    NihuawayKVService(int port, ShardRouter shardRouter) {
        this.port = port;
        this.shardRouter = shardRouter;
    }

    @Override
    public void start() {
        try {
            InetSocketAddress addr = new InetSocketAddress(port);
            server = HttpServer.create(addr, 0);
            server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            registerContexts();
            server.start();
        } catch (Exception e) {
            if (log.isErrorEnabled()) {
                log.error(e.getMessage());
            }
        }
    }

    @Override
    public void stop() {
        if (server != null) {
            server.stop(0);
        } else {
            if (log.isWarnEnabled()) {
                log.warn("Server is not started");
            }
        }
    }

    private void registerContexts() throws IOException {
        Path baseDir = Path.of("./storage/" + port);
        EntityDao dao = new EntityDao(baseDir);
        server.createContext("/v0/entity", new EntityHandler(dao, shardRouter));
        server.createContext("/v0/status", new PingHandler(dao));
    }
}
