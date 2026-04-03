package company.vk.edu.distrib.compute.tadzhnahal;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.net.InetSocketAddress;

public class TadzhnahalKVService implements KVService {
    private final int port;
    private final Dao<byte[]> dao;
    private HttpServer server;

    public TadzhnahalKVService(int port, Dao<byte[]> dao) {
        this.port = port;
        this.dao = dao;
    }

    @Override
    public void start() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void stop() {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
