package company.vk.edu.distrib.compute.expanse.handler;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.expanse.context.AppContextUtils;
import company.vk.edu.distrib.compute.expanse.service.HttpService;
import company.vk.edu.distrib.compute.expanse.service.impl.HttpServiceImpl;

import java.io.IOException;

public class StatusEndpointHandler implements HttpHandler {
    private final HttpService<String, byte[]> httpService;

    public StatusEndpointHandler() {
        this.httpService = AppContextUtils.getBean(HttpServiceImpl.class);
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!httpService.isOkStatus()) {
            exchange.sendResponseHeaders(503, -1);
            return;
        }
        exchange.sendResponseHeaders(200, -1);
    }
}
