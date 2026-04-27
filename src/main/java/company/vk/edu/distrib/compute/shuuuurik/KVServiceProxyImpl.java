package company.vk.edu.distrib.compute.shuuuurik;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.shuuuurik.routing.NodeRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import static company.vk.edu.distrib.compute.shuuuurik.util.HttpUtils.parseQueryParams;

/**
 * HTTP-сервер, реализующий KVService с поддержкой шардирования.
 */
public class KVServiceProxyImpl implements KVService {

    private static final Logger log = LoggerFactory.getLogger(KVServiceProxyImpl.class);

    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";

    private static final String PATH_STATUS = "/v0/status";
    private static final String PATH_ENTITY = "/v0/entity";

    /**
     * Таймаут проксируемых запросов.
     */
    private static final Duration PROXY_TIMEOUT = Duration.ofSeconds(3);

    private final Dao<byte[]> dao;
    private final int port;
    private final String selfEndpoint; // "http://localhost:8080" - адрес этого узла
    private final List<String> allEndpoints; // все ноды кластера
    private final NodeRouter router;
    private final AtomicBoolean started = new AtomicBoolean(false);
    /**
     * HTTP-клиент для проксирования запросов. Пересоздаётся при каждом start() вместе с сервером.
     */
    private HttpClient httpClient;
    /**
     * Текущий экземпляр HttpServer. Пересоздаётся при каждом start().
     */
    private HttpServer server;

    /**
     * Создаёт HTTP-сервер с поддержкой шардирования.
     *
     * @param port         порт, на котором слушает этот узел
     * @param dao          локальное хранилище данного узла
     * @param allEndpoints список всех endpoint'ов кластера (включая свой)
     * @param router       алгоритм маршрутизации
     */
    public KVServiceProxyImpl(
            int port,
            Dao<byte[]> dao,
            List<String> allEndpoints,
            NodeRouter router
    ) {
        this.port = port;
        this.dao = dao;
        this.selfEndpoint = "http://localhost:" + port;
        this.allEndpoints = List.copyOf(allEndpoints);
        this.router = router;
    }

    /**
     * Запускает узел: создаёт новый HttpServer и HttpClient, биндит сервер на порт.
     * Можно вызывать повторно после stop() - каждый раз создаётся свежий сервер и клиент.
     */
    @Override
    public void start() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("Service already started");
        }
        HttpClient newHttpClient = HttpClient.newHttpClient();
        try {
            HttpServer newServer = HttpServer.create();
            newServer.createContext(PATH_STATUS, this::handleStatus);
            newServer.createContext(PATH_ENTITY, this::handleEntity);
            newServer.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 0);
            newServer.start();
            this.httpClient = newHttpClient;
            this.server = newServer;
            log.info("Started shard node {}", selfEndpoint);
        } catch (IOException e) {
            started.set(false);
            newHttpClient.close();
            throw new UncheckedIOException("Failed to start node " + selfEndpoint, e);
        }
    }

    /**
     * Останавливает узел. После остановки можно вызвать start() снова.
     * DAO при остановке НЕ закрывается - данные сохраняются между рестартами ноды.
     */
    @Override
    public void stop() {
        if (!started.compareAndSet(true, false)) {
            throw new IllegalStateException("Service is not started");
        }
        server.stop(0);
        httpClient.close();
        log.info("Stopped shard node {}", selfEndpoint);
    }

    private void handleStatus(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!METHOD_GET.equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            exchange.sendResponseHeaders(200, -1);
        }
    }

    private void handleEntity(HttpExchange exchange) throws IOException {
        try (exchange) {
            try {
                Map<String, String> params = parseQueryParams(exchange);
                String id = params.get("id");

                if (id == null || id.isEmpty()) {
                    exchange.sendResponseHeaders(400, -1);
                    return;
                }

                String targetEndpoint = router.route(id, allEndpoints);

                if (selfEndpoint.equals(targetEndpoint)) {
                    handleLocally(exchange, id);
                } else {
                    // Запрос для другой ноды
                    proxyRequest(exchange, id, targetEndpoint);
                }

            } catch (NoSuchElementException e) {
                exchange.sendResponseHeaders(404, -1);
            } catch (IllegalArgumentException e) {
                exchange.sendResponseHeaders(400, -1);
            } catch (Exception e) {
                log.error("Unexpected error on {}", selfEndpoint, e);
                exchange.sendResponseHeaders(500, -1);
            }
        }
    }

    /**
     * Обрабатывает запрос локально, используя собственный Dao.
     */
    private void handleLocally(HttpExchange exchange, String id) throws IOException {
        switch (exchange.getRequestMethod()) {
            case METHOD_GET -> handleLocalGet(exchange, id);
            case METHOD_PUT -> handleLocalPut(exchange, id);
            case METHOD_DELETE -> handleLocalDelete(exchange, id);
            default -> exchange.sendResponseHeaders(405, -1);
        }
    }

    private void handleLocalGet(HttpExchange exchange, String id) throws IOException {
        byte[] value = dao.get(id);
        exchange.sendResponseHeaders(200, value.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(value);
        }
    }

    private void handleLocalPut(HttpExchange exchange, String id) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        dao.upsert(id, body);
        exchange.sendResponseHeaders(201, -1);
    }

    private void handleLocalDelete(HttpExchange exchange, String id) throws IOException {
        dao.delete(id);
        exchange.sendResponseHeaders(202, -1);
    }

    /**
     * Проксирует запрос на целевую ноду и транслирует ответ обратно клиенту.
     *
     * @param exchange       входящий HTTP-обмен
     * @param id             ключ запроса
     * @param targetEndpoint endpoint ноды, ответственной за ключ
     */
    private void proxyRequest(HttpExchange exchange, String id, String targetEndpoint) throws IOException {
        String encodedId = URLEncoder.encode(id, StandardCharsets.UTF_8).replace("+", "%20");
        String targetUrl = targetEndpoint + "/v0/entity?id=" + encodedId;
        String method = exchange.getRequestMethod();

        try {
            HttpRequest proxyRequest = buildProxyRequest(method, targetUrl, exchange);
            HttpResponse<byte[]> proxyResponse = httpClient.send(
                    proxyRequest,
                    HttpResponse.BodyHandlers.ofByteArray()
            );

            sendProxyResponse(exchange, proxyResponse);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Proxy interrupted for key={} target={}", id, targetEndpoint);
            exchange.sendResponseHeaders(500, -1);
        }
    }

    /**
     * Транслирует ответ целевой ноды обратно клиенту.
     *
     * @param exchange      входящий HTTP-обмен (клиентское соединение)
     * @param proxyResponse ответ от целевой ноды
     */
    private void sendProxyResponse(HttpExchange exchange, HttpResponse<byte[]> proxyResponse)
            throws IOException {
        int statusCode = proxyResponse.statusCode();
        byte[] body = proxyResponse.body();

        if (body != null) {
            exchange.sendResponseHeaders(statusCode, body.length);
            if (body.length > 0) {
                try (OutputStream out = exchange.getResponseBody()) {
                    out.write(body);
                }
            }
        } else {
            exchange.sendResponseHeaders(statusCode, -1);
        }
    }

    /**
     * Строит HTTP-запрос для проксирования.
     *
     * @param method    HTTP-метод оригинального запроса
     * @param targetUrl URL целевой ноды
     * @param exchange  входящий обмен (для чтения тела PUT-запроса)
     * @return готовый HttpRequest для отправки
     */
    private HttpRequest buildProxyRequest(String method, String targetUrl, HttpExchange exchange)
            throws IOException {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(targetUrl))
                .timeout(PROXY_TIMEOUT);

        return switch (method) {
            case METHOD_GET -> builder.GET().build();
            case METHOD_PUT -> {
                byte[] body = exchange.getRequestBody().readAllBytes();
                yield builder.PUT(HttpRequest.BodyPublishers.ofByteArray(body)).build();
            }
            case METHOD_DELETE -> builder.DELETE().build();
            default -> throw new IllegalArgumentException("Unsupported method: " + method);
        };
    }
}
