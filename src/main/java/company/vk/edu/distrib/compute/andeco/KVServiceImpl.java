package company.vk.edu.distrib.compute.andeco;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.andeco.replica.Controller;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class KVServiceImpl implements KVService {
    protected int currentPort;
    protected Controller entityController;
    protected Controller statusController;
    protected final HttpServer server;

    public KVServiceImpl(int currentPort, Controller entityController, Controller statusController) throws IOException {
        this.currentPort = currentPort;
        this.entityController = entityController;
        this.statusController = statusController;
        this.server = HttpServer.create(new InetSocketAddress(currentPort), 0);
        this.server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
    }

    public int port() {
        return currentPort;
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
