package company.vk.edu.distrib.compute.golubtsov_pavel;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.util.NoSuchElementException;

public class ErrorHttpHandler implements HttpHandler {
    private final HttpHandler delegate;

    public ErrorHttpHandler(HttpHandler delegate) {
        this.delegate = delegate;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            delegate.handle(exchange);
        } catch (IllegalArgumentException exp) {
            exchange.sendResponseHeaders(400, 0);
        } catch (NoSuchElementException exp) {
            exchange.sendResponseHeaders(404,0);
        } catch (IOException exp) {
            exchange.sendResponseHeaders(500,0);
        }
        exchange.close();
    }
}
