package company.vk.edu.distrib.compute.martinez1337.controller;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.martinez1337.sharding.ShardingStrategy;

import java.io.IOException;
import java.util.List;

public class StatusHandler extends BaseHttpHandler {

    public StatusHandler(List<String> clusterEndpoints, ShardingStrategy sharding) {
        super(clusterEndpoints, sharding);
    }

    @Override
    protected void handleGet(HttpExchange exchange) throws IOException {
        log.info("Handling GET request");
        exchange.sendResponseHeaders(ResponseStatus.OK.getCode(), 0);
    }
}
