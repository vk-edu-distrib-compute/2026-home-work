package company.vk.edu.distrib.compute.bobridze5.handlers;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;

public class StatusHandler extends AbstractHandler {

    @Override
    protected void handleGet(HttpExchange exchange) throws IOException {
        sendResponse(exchange, HttpURLConnection.HTTP_OK, "OK".getBytes(StandardCharsets.UTF_8));
    }
}
