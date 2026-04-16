package company.vk.edu.distrib.compute.andeco;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.andeco.controller.EntityController;
import company.vk.edu.distrib.compute.andeco.controller.StatusController;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class KVServiceImpl implements KVService {
    protected EntityController entityController;
    protected StatusController statusController;
    protected final HttpServer server;

    public KVServiceImpl(int port, Dao<byte[]> dao) throws IOException {
        entityController = new EntityController(dao);
        statusController = new StatusController();
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
    }

    public void registerDefault() {
        server.createContext(ServerConfigConstants.API_PATH + ServerConfigConstants.ENTITY_PATH,
                entityController::processRequest);
        server.createContext(ServerConfigConstants.API_PATH + ServerConfigConstants.STATUS_PATH,
                statusController::processRequest);
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
    }
}
