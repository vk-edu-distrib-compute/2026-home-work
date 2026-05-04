package company.vk.edu.distrib.compute.ce_fello;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;

final class CeFelloClusterStatusHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!CeFelloClusterHttpHelper.STATUS_PATH.equals(exchange.getRequestURI().getPath())) {
            CeFelloClusterHttpHelper.sendEmpty(exchange, 404);
            return;
        }

        if (!CeFelloClusterHttpHelper.GET_METHOD.equals(exchange.getRequestMethod())) {
            CeFelloClusterHttpHelper.sendEmpty(exchange, 405);
            return;
        }

        CeFelloClusterHttpHelper.sendEmpty(exchange, 200);
    }
}
