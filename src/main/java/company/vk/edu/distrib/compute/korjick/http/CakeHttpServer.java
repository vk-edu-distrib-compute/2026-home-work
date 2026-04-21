package company.vk.edu.distrib.compute.korjick.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;

public class CakeHttpServer {
    private static final Logger log = LoggerFactory.getLogger(CakeHttpServer.class);

    private static final int BACKLOG_VALUE = 0;
    private static final int STOP_SERVER_DELAY = 1;

    private final HttpServer server;

    public CakeHttpServer(String endpoint, int port) throws IOException {
        var inetSocketAddress = new InetSocketAddress(endpoint, port);
        this.server = HttpServer.create(inetSocketAddress, BACKLOG_VALUE);
    }

    public void addContext(String path, HttpHandler handler) {
        this.server.createContext(path, ErrorHandler.fromDelegate(handler));
    }

    public void startServer() {
        this.server.start();
        log.info("HTTP server started");
    }

    public void stopServer() {
        log.info("Stopping HTTP server");
        this.server.stop(STOP_SERVER_DELAY);
        log.info("HTTP server stopped");
    }

    public InetSocketAddress getAddress() {
        return this.server.getAddress();
    }

    public static final class ErrorHandler implements HttpHandler {

        private final HttpHandler delegate;

        private ErrorHandler(HttpHandler delegate) {
            this.delegate = delegate;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try (exchange) {
                try {
                    delegate.handle(exchange);
                } catch (IllegalArgumentException e) {
                    exchange.sendResponseHeaders(Constants.HTTP_STATUS_BAD_REQUEST,
                            Constants.EMPTY_BODY_LENGTH);
                } catch (NoSuchElementException e) {
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
