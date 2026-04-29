package company.vk.edu.distrib.compute.korjick.adapters.input.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsServer;
import company.vk.edu.distrib.compute.korjick.core.application.exception.EntityNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

public class CakeHttpServer {
    private static final Logger log = LoggerFactory.getLogger(CakeHttpServer.class);

    private static final int BACKLOG_VALUE = 0;
    private static final int STOP_SERVER_DELAY = 1;

    private final HttpServer server;

    public CakeHttpServer(String host, int port) throws IOException {
        var inetSocketAddress = new InetSocketAddress(host, port);
        this.server = HttpServer.create(inetSocketAddress, BACKLOG_VALUE);
    }

    public void register(String path, HttpHandler handler) {
        this.server.createContext(path, ErrorHandler.fromDelegate(handler));
    }

    public void start() {
        this.server.start();
        log.info("HTTP server started");
    }

    public void stop() {
        this.server.stop(STOP_SERVER_DELAY);
        log.info("HTTP server stopped");
    }

    public String getEndpoint() {
        var address = this.server.getAddress();
        return String.format("%s%s:%s",
                this.server instanceof HttpsServer ? "https://" : "http://",
                address.getHostString(),
                address.getPort());
    }

    private record ErrorHandler(HttpHandler delegate) implements HttpHandler {

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try (exchange) {
                try {
                    delegate.handle(exchange);
                } catch (IllegalArgumentException e) {
                    exchange.sendResponseHeaders(Constants.HTTP_STATUS_BAD_REQUEST,
                            Constants.EMPTY_BODY_LENGTH);
                } catch (EntityNotFoundException e) {
                    exchange.sendResponseHeaders(Constants.HTTP_STATUS_NOT_FOUND,
                            Constants.EMPTY_BODY_LENGTH);
                } catch (Exception e) {
                    exchange.sendResponseHeaders(Constants.HTTP_STATUS_INTERNAL_ERROR,
                            Constants.EMPTY_BODY_LENGTH);
                }
            }
        }

        public static ErrorHandler fromDelegate(HttpHandler delegate) {
            return new ErrorHandler(delegate);
        }
    }
}
