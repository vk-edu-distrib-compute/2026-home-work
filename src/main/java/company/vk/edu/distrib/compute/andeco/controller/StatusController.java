package company.vk.edu.distrib.compute.andeco.controller;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.andeco.Method;
import company.vk.edu.distrib.compute.andeco.ServerConfigConstants;
import company.vk.edu.distrib.compute.andeco.replica.Controller;

import java.io.IOException;
import java.net.HttpURLConnection;

public class StatusController implements Controller {

    public static final String REQUEST_MAPPING =
            ServerConfigConstants.API_PATH + ServerConfigConstants.STATUS_PATH;

    @Override
    public void processRequest(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (Method.GET.name().equals(exchange.getRequestMethod())
                    && REQUEST_MAPPING.equals(exchange.getRequestURI().getPath())) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, -1);
            } else {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_METHOD, -1);
            }
        }
    }
}
