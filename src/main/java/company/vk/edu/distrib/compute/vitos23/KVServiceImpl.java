package company.vk.edu.distrib.compute.vitos23;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.vitos23.exception.AcknowledgementException;
import company.vk.edu.distrib.compute.vitos23.util.HttpCodes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Executors;

import static company.vk.edu.distrib.compute.vitos23.util.HttpUtils.NO_BODY_RESPONSE_LENGTH;
import static company.vk.edu.distrib.compute.vitos23.util.HttpUtils.extractQueryParams;

public class KVServiceImpl implements KVService {

    private static final Logger log = LoggerFactory.getLogger(KVServiceImpl.class);

    private final HttpServer server;
    private final EntityRequestProcessor entityRequestProcessor;

    public KVServiceImpl(int port, EntityRequestProcessor entityRequestProcessor) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        this.entityRequestProcessor = entityRequestProcessor;
        server.createContext("/v0/status", this::handleStatusCheck);
        server.createContext("/v0/entity", this::dispatchEntityRequest);
    }

    private void handleStatusCheck(HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(HttpCodes.OK, NO_BODY_RESPONSE_LENGTH);
        exchange.close();
    }

    @SuppressWarnings("PMD.UseTryWithResources") // Replacement is not possible
    private void dispatchEntityRequest(HttpExchange exchange) throws IOException {
        try {
            handleEntityRequest(exchange);
        } catch (NoSuchElementException e) {
            exchange.sendResponseHeaders(HttpCodes.NOT_FOUND, NO_BODY_RESPONSE_LENGTH);
        } catch (IllegalArgumentException e) {
            exchange.sendResponseHeaders(HttpCodes.BAD_REQUEST, NO_BODY_RESPONSE_LENGTH);
        } catch (AcknowledgementException e) {
            exchange.sendResponseHeaders(HttpCodes.SERVICE_UNAVAILABLE, NO_BODY_RESPONSE_LENGTH);
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            if (log.isErrorEnabled()) {
                log.error("Failed to handle request", e);
            }
            exchange.sendResponseHeaders(HttpCodes.INTERNAL_ERROR, NO_BODY_RESPONSE_LENGTH);
        } finally {
            exchange.close();
        }
    }

    private void handleEntityRequest(HttpExchange exchange) throws IOException, InterruptedException {
        Map<String, String> queryParams = extractQueryParams(exchange.getRequestURI().getQuery());
        String id = queryParams.get("id");
        if (id == null) {
            throw new IllegalArgumentException("Missing query parameter 'id'");
        }

        switch (exchange.getRequestMethod()) {
            case "GET" -> entityRequestProcessor.handleGet(exchange, id, queryParams);
            case "PUT" -> entityRequestProcessor.handlePut(exchange, id, queryParams);
            case "DELETE" -> entityRequestProcessor.handleDelete(exchange, id, queryParams);
            case null, default -> exchange.sendResponseHeaders(HttpCodes.METHOD_NOT_ALLOWED, NO_BODY_RESPONSE_LENGTH);
        }
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(0);
    }
}
