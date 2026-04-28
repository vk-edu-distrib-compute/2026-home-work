package company.vk.edu.distrib.compute.usl;

import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.util.NoSuchElementException;

final class EntityErrorResponder {
    private EntityErrorResponder() {
    }

    static void respond(HttpExchange exchange, Exception exception) throws IOException {
        if (exception instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        ExchangeResponses.sendEmpty(exchange, responseStatus(exception));
    }

    private static int responseStatus(Exception exception) {
        if (exception instanceof IllegalArgumentException) {
            return 400;
        }
        if (exception instanceof NoSuchElementException) {
            return 404;
        }
        if (exception instanceof MethodNotAllowedException) {
            return 405;
        }
        if (exception instanceof IOException || exception instanceof InterruptedException) {
            return 503;
        }
        return 500;
    }
}
