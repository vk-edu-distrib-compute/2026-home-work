package company.vk.edu.distrib.compute.expanse.handler;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.expanse.context.AppContextUtils;
import company.vk.edu.distrib.compute.expanse.service.KVStorageService;
import company.vk.edu.distrib.compute.expanse.service.impl.KVStorageServiceImpl;

import java.io.IOException;

public class StatusEndpointHandler implements HttpHandler {
    private final KVStorageService<String, byte[]> kvStorageService;

    public StatusEndpointHandler() {
        this.kvStorageService = AppContextUtils.getBean(KVStorageServiceImpl.class);
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!kvStorageService.isOkStatus()) {
            exchange.sendResponseHeaders(503, -1);
            return;
        }
        exchange.sendResponseHeaders(200, -1);
    }
}
