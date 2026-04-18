package company.vk.edu.distrib.compute.nihuaway00;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.nihuaway00.http.EntityHandler;
import company.vk.edu.distrib.compute.nihuaway00.http.PingHandler;
import company.vk.edu.distrib.compute.nihuaway00.sharding.ShardRouter;
import company.vk.edu.distrib.compute.nihuaway00.sharding.ShardingStrategy;
import company.vk.edu.distrib.compute.nihuaway00.storage.EntityDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.file.Path;

public class NihuawayKVService implements company.vk.edu.distrib.compute.KVService {
    private static final Logger log = LoggerFactory.getLogger(NihuawayKVService.class);

    private HttpClient client;
    private HttpServer server;
    private final ShardingStrategy shardingStrategy;
    int port;

    NihuawayKVService(int port, ShardingStrategy shardingStrategy) {
        this.port = port;
        this.shardingStrategy = shardingStrategy;
    }

    @Override
    public void start() {
        try {
            InetSocketAddress addr = new InetSocketAddress(port);
            server = HttpServer.create(addr, 0);
            client = HttpClient.newHttpClient();
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
        ShardRouter shardRouter = new ShardRouter("http://localhost:" + port, shardingStrategy, client);

        server.createContext("/v0/entity", new EntityHandler(dao, shardRouter));
        server.createContext("/v0/status", new PingHandler(dao));
    }
}
