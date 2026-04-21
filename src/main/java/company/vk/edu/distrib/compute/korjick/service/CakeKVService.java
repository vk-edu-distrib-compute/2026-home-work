package company.vk.edu.distrib.compute.korjick.service;

import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.korjick.http.CakeHttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CakeKVService implements KVService {
    private static final Logger log = LoggerFactory.getLogger(CakeKVService.class);

    private final CakeHttpServer cakeServer;
    private final Dao<byte[]> dao;

    public CakeKVService(String host,
                         int port,
                         Dao<byte[]> dao) throws IOException {
        this.cakeServer = new CakeHttpServer(host, port);
        this.dao = dao;
    }

    @Override
    public void start() {
        cakeServer.startServer();
    }

    @Override
    public void stop() {
        cakeServer.stopServer();
        try {
            dao.close();
            log.info("DAO closed");
        } catch (IOException e) {
            log.error("Failed to close DAO", e);
        }
    }

    public void addContext(String path, HttpHandler handler) {
        this.cakeServer.addContext(path, handler);
    }

    public String getEndpoint() {
        var address = this.cakeServer.getAddress();
        return "http://" + address.getHostString() + ":" + address.getPort();
    }
}
