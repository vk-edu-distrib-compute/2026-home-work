package company.vk.edu.distrib.compute.martinez1337.controller;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;

public class StatusHandler extends BaseHttpHandler {

    @Override
    protected void handleGet(HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(ResponseStatus.OK.getCode(), 0);
    }
}
