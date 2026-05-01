package company.vk.edu.distrib.compute.expanse.core;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.expanse.context.WebContextUtils;

import java.io.IOException;
import java.net.InetSocketAddress;

public class HttpKVService implements KVService {
    private final HttpServer httpServer;

    public HttpKVService(int port) throws IOException {
        httpServer = HttpServer.create(new InetSocketAddress(port), 0);
        WebContextUtils.init(httpServer);
    }

    @Override
    public void start() {
        httpServer.start();
    }

    @Override
    public void stop() {
        httpServer.stop(0);
    }
}
