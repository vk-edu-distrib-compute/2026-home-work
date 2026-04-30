package company.vk.edu.distrib.compute.kruchinina.sharding;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import static company.vk.edu.distrib.compute.kruchinina.sharding.ServerUtils.*;

public class SimpleKVService implements KVService {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleKVService.class);

    private static final int STATUS_OK = 200;
    private static final int STATUS_CREATED = 201;
    private static final int STATUS_ACCEPTED = 202;
    private static final int STATUS_BAD_REQUEST = 400;
    private static final int STATUS_NOT_FOUND = 404;
    private static final int STATUS_METHOD_NOT_ALLOWED = 405;
    private static final int STATUS_INTERNAL_ERROR = 500;
    private static final String MISSING_ID_MSG = "Missing id";
    private static final String ID_PARAM = "id";
    private static final String CONTENT_TYPE = "Content-Type";
    private static final String OCTET_STREAM = "application/octet-stream";
    private static final String AMPERSAND = "&";
    private static final String EQUALS = "=";
    private static final String EMPTY = "";

    private final int port;
    private final Dao<byte[]> dao;
    private HttpServer server;
    private boolean started;

    // Кластерная конфигурация
    private final List<String> clusterNodes; //все узлы, включая себя
    private final String selfAddress;
    private final ShardingStrategy shardingStrategy;
    private final ClusterHttpClient httpClient;

    //Конструктор для одиночного режима
    public SimpleKVService(int port, Dao<byte[]> dao) {
        this(port, dao, null, null, null);
    }

    //Конструктор для кластерного режима
    public SimpleKVService(int port, Dao<byte[]> dao,
                           List<String> clusterNodes,
                           String selfAddress,
                           ShardingStrategy shardingStrategy) {
        this.port = port;
        this.dao = dao;
        this.clusterNodes = clusterNodes;
        this.selfAddress = selfAddress;
        this.shardingStrategy = shardingStrategy;
        this.httpClient = new ClusterHttpClient();
    }

    @Override
    public void start() {
        if (started) {
            throw new IllegalStateException("Service already started");
        }
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/v0/status", new StatusHandler());
            server.createContext("/v0/entity", new EntityHandler());
            server.setExecutor(null);
            server.start();
            started = true;
            if (LOG.isInfoEnabled()) {
                LOG.info("KVService started on port {} (cluster mode: {})", port, isClusterMode());
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start HTTP server on port " + port, e);
        }
    }

    @Override
    public void stop() {
        if (!started) {
            throw new IllegalStateException("Service not started");
        }
        if (server != null) {
            server.stop(0);
            LOG.info("KVService stopped on port {}", port);
        }
        try {
            dao.close();
        } catch (IOException e) {
            LOG.error("Error closing DAO", e);
        }
        started = false;
    }

    private boolean isClusterMode() {
        return clusterNodes != null && !clusterNodes.isEmpty() && shardingStrategy != null;
    }

    private static final class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!METHOD_GET.equalsIgnoreCase(exchange.getRequestMethod())) {
                sendResponse(exchange, STATUS_METHOD_NOT_ALLOWED, new byte[0]);
                return;
            }
            sendResponse(exchange, STATUS_OK, "OK".getBytes(StandardCharsets.UTF_8));
        }
    }

    private final class EntityHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String id = extractId(exchange);
            if (id == null) {
                sendResponse(exchange, STATUS_BAD_REQUEST, MISSING_ID_MSG.getBytes(StandardCharsets.UTF_8));
                return;
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug("Received {} request for id={}", exchange.getRequestMethod(), id);
            }

            try {
                // Если не кластерный режим – обрабатываем локально
                if (!isClusterMode()) {
                    dispatchRequest(exchange, id);
                    return;
                }

                // Кластерный режим
                logClusterInfo();
                String responsibleNode = shardingStrategy.selectNode(id, clusterNodes);
                logResponsibleNode(id, responsibleNode);

                // Если текущий узел ответственный – обрабатываем локально
                if (responsibleNode.equals(selfAddress)) {
                    dispatchRequest(exchange, id);
                    return;
                }

                // Иначе проксируем запрос на ответственный узел
                logProxyAction(responsibleNode);
                httpClient.proxyRequest(responsibleNode, exchange);

            } catch (Exception e) {
                LOG.error("Error handling request for id={}", id, e);
                handleException(exchange, e);
            }
        }

        // Вспомогательные методы для логирования
        private void logClusterInfo() {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Cluster mode, nodes: {}, self: {}", clusterNodes, selfAddress);
            }
        }

        private void logResponsibleNode(String id, String node) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Responsible node for {} is {}", id, node);
            }
        }

        private void logProxyAction(String node) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Proxying to {}", node);
            }
        }

        private String extractId(HttpExchange exchange) {
            Map<String, String> queryParams = parseQuery(exchange.getRequestURI().getQuery());
            String id = queryParams.get(ID_PARAM);
            return (id == null || id.isEmpty()) ? null : id;
        }

        private void dispatchRequest(HttpExchange exchange, String id) throws IOException {
            String method = exchange.getRequestMethod();
            if (METHOD_GET.equals(method)) {
                handleGet(exchange, id);
            } else if (METHOD_PUT.equals(method)) {
                handlePut(exchange, id);
            } else if (METHOD_DELETE.equals(method)) {
                handleDelete(exchange, id);
            } else {
                sendResponse(exchange, STATUS_METHOD_NOT_ALLOWED, new byte[0]);
            }
        }

        private void handleException(HttpExchange exchange, Exception e) throws IOException {
            if (LOG.isInfoEnabled()) {
                LOG.info("handleException called for: {}", e.getClass().getSimpleName());
            }
            if (e instanceof IllegalArgumentException) {
                sendResponse(exchange, STATUS_BAD_REQUEST, e.getMessage().getBytes(StandardCharsets.UTF_8));
            } else if (e instanceof NoSuchElementException) {
                sendResponse(exchange, STATUS_NOT_FOUND, new byte[0]);
            } else if (e instanceof IOException) {
                LOG.error("IO error", e);
                sendResponse(exchange, STATUS_INTERNAL_ERROR, new byte[0]);
            } else {
                LOG.error("Unexpected error", e);
                sendResponse(exchange, STATUS_INTERNAL_ERROR, new byte[0]);
            }
        }

        private void handleGet(HttpExchange exchange, String id) throws IOException {
            byte[] data = dao.get(id);
            exchange.getResponseHeaders().set(CONTENT_TYPE, OCTET_STREAM);
            sendResponse(exchange, STATUS_OK, data);
        }

        private void handlePut(HttpExchange exchange, String id) throws IOException {
            byte[] body = exchange.getRequestBody().readAllBytes();
            dao.upsert(id, body);
            sendResponse(exchange, STATUS_CREATED, new byte[0]);
        }

        private void handleDelete(HttpExchange exchange, String id) throws IOException {
            dao.delete(id);
            sendResponse(exchange, STATUS_ACCEPTED, new byte[0]);
        }

        private Map<String, String> parseQuery(String query) {
            Map<String, String> params = new ConcurrentHashMap<>();
            if (query == null) {
                return params;
            }
            for (String pair : query.split(AMPERSAND)) {
                int eq = pair.indexOf(EQUALS);
                String key;
                String value;
                if (eq > 0) {
                    key = decode(pair.substring(0, eq));
                    value = decode(pair.substring(eq + 1));
                } else {
                    key = decode(pair);
                    value = EMPTY;
                }
                params.put(key, value);
            }
            return params;
        }

        private String decode(String s) {
            return URLDecoder.decode(s, StandardCharsets.UTF_8);
        }
    }

    private static void sendResponse(HttpExchange exchange, int statusCode, byte[] body) throws IOException {
        exchange.sendResponseHeaders(statusCode, body.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(body);
        }
        exchange.close();
    }
}
