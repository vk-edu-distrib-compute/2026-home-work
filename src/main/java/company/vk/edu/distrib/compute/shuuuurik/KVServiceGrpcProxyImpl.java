package company.vk.edu.distrib.compute.shuuuurik;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.shuuuurik.grpc.InternalGrpcKVClient;
import company.vk.edu.distrib.compute.shuuuurik.grpc.InternalGrpcKVService;
import company.vk.edu.distrib.compute.shuuuurik.routing.NodeRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static company.vk.edu.distrib.compute.shuuuurik.util.HttpUtils.parseQueryParams;

/**
 * HTTP-сервер с поддержкой шардирования.
 * Внешние клиенты общаются по HTTP.
 * Запросы к другим нодам кластера проксируются через gRPC (внутренний транспорт).
 *
 * <p>Каждый узел поднимает два сервера:
 * <ul>
 *   <li>HTTP-сервер на {@code httpPort} - для внешних клиентов</li>
 *   <li>gRPC-сервер на {@code grpcPort} - для внутреннего трафика кластера</li>
 * </ul>
 *
 * <p>Поддерживает повторный запуск: при каждом {@link #start()} пересоздаются
 * HTTP-сервер и gRPC-клиенты (одноразовые объекты).
 */
public class KVServiceGrpcProxyImpl implements KVService {

    private static final Logger log = LoggerFactory.getLogger(KVServiceGrpcProxyImpl.class);

    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";

    private static final String PATH_STATUS = "/v0/status";
    private static final String PATH_ENTITY = "/v0/entity";

    private final Dao<byte[]> dao;
    private final int httpPort;

    /**
     * Endpoint этого узла: "http://localhost:8080".
     */
    private final String selfHttpEndpoint;

    /**
     * Список всех HTTP-эндпоинтов кластера (включая свой).
     * Используется роутером для выбора целевой ноды.
     */
    private final List<String> allHttpEndpoints;

    /**
     * Карта HTTP-endpoint -> grpcPort. Используется при пересоздании клиентов.
     */
    private final Map<String, Integer> grpcPortMap;

    /**
     * Алгоритм маршрутизации: по ключу выбирает HTTP-endpoint целевой ноды.
     */
    private final NodeRouter router;

    /**
     * gRPC-сервер этой ноды (принимает внутренние запросы от других нод).
     * Поддерживает повторный запуск.
     */
    private final InternalGrpcKVService grpcServer;

    /**
     * gRPC-клиенты к другим нодам кластера. Пересоздаются при каждом start()
     * Ключ - HTTP-endpoint ноды, значение - gRPC-клиент к ней.
     * Для собственного endpoint'а клиент не создаётся.
     */
    private Map<String, InternalGrpcKVClient> grpcClients;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private HttpServer httpServer;

    /**
     * Создаёт узел кластера с HTTP-интерфейсом для внешних клиентов
     * и gRPC-транспортом для внутренних запросов.
     *
     * @param httpPort         порт HTTP-сервера этого узла
     * @param grpcPort         порт gRPC-сервера этого узла
     * @param dao              локальное хранилище
     * @param allHttpEndpoints HTTP-эндпоинты всех нод кластера
     * @param grpcPortMap      HTTP-endpoint -> grpcPort для каждой ноды
     * @param router           алгоритм маршрутизации
     */
    public KVServiceGrpcProxyImpl(
            int httpPort,
            int grpcPort,
            Dao<byte[]> dao,
            List<String> allHttpEndpoints,
            Map<String, Integer> grpcPortMap,
            NodeRouter router
    ) {
        this.httpPort = httpPort;
        this.dao = dao;
        this.selfHttpEndpoint = "http://localhost:" + httpPort;
        this.allHttpEndpoints = List.copyOf(allHttpEndpoints);
        this.grpcPortMap = Map.copyOf(grpcPortMap);
        this.router = router;
        this.grpcServer = new InternalGrpcKVService(grpcPort, dao);
    }

    /**
     * Запускает HTTP-сервер, gRPC-сервер и создаёт gRPC-клиенты к другим нодам.
     * Можно вызывать повторно после {@link #stop()}.
     */
    @Override
    public void start() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("Service already started");
        }
        try {
            grpcServer.start();

            grpcClients = buildGrpcClients();

            HttpServer newHttpServer = HttpServer.create();
            newHttpServer.createContext(PATH_STATUS, this::handleStatus);
            newHttpServer.createContext(PATH_ENTITY, this::handleEntity);
            newHttpServer.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), httpPort), 0);
            newHttpServer.start();
            this.httpServer = newHttpServer;

            log.info("Node started: http={}", selfHttpEndpoint);
        } catch (IOException e) {
            started.set(false);
            throw new UncheckedIOException("Failed to start node " + selfHttpEndpoint, e);
        }
    }

    /**
     * Останавливает HTTP-сервер, gRPC-сервер и все gRPC-клиенты.
     * После остановки можно вызвать {@link #start()} снова.
     * DAO при остановке НЕ закрывается - данные сохраняются между рестартами ноды.
     */
    @Override
    public void stop() {
        if (!started.compareAndSet(true, false)) {
            throw new IllegalStateException("Service is not started");
        }
        httpServer.stop(0);
        grpcServer.stop();
        stopGrpcClients();
        log.info("Node stopped: http={}", selfHttpEndpoint);
    }

    /**
     * Создаёт новые gRPC-клиенты ко всем нодам кластера, кроме себя.
     *
     * @return HTTP-endpoint -> gRPC-клиент
     */
    private Map<String, InternalGrpcKVClient> buildGrpcClients() {
        Map<String, InternalGrpcKVClient> clients = new ConcurrentHashMap<>();
        for (Map.Entry<String, Integer> entry : grpcPortMap.entrySet()) {
            String httpEndpoint = entry.getKey();
            if (!httpEndpoint.equals(selfHttpEndpoint)) {
                clients.put(httpEndpoint, createGrpcClient(entry.getValue()));
            }
        }
        return clients;
    }

    /**
     * Создаёт gRPC-клиент для ноды по её grpcPort.
     *
     * @param grpcPort порт gRPC-сервера целевой ноды
     * @return новый {@link InternalGrpcKVClient}
     */
    private InternalGrpcKVClient createGrpcClient(int grpcPort) {
        return new InternalGrpcKVClient("localhost:" + grpcPort);
    }

    /**
     * Останавливает все gRPC-клиенты. Ошибки логируются, но не пробрасываются,
     * чтобы один сбойный клиент не помешал остановить остальные.
     */
    private void stopGrpcClients() {
        if (grpcClients == null) {
            return;
        }
        for (Map.Entry<String, InternalGrpcKVClient> entry : grpcClients.entrySet()) {
            try {
                entry.getValue().stop();
            } catch (Exception e) {
                if (log.isWarnEnabled()) {
                    log.warn("Error stopping gRPC client for {}", entry.getKey(), e);
                }
            }
        }
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

                String targetEndpoint = router.route(id, allHttpEndpoints);

                if (selfHttpEndpoint.equals(targetEndpoint)) {
                    handleLocally(exchange, id);
                } else {
                    // Запрос для другой ноды
                    handleViaGrpc(exchange, id, targetEndpoint);
                }

            } catch (NoSuchElementException e) {
                exchange.sendResponseHeaders(404, -1);
            } catch (IllegalArgumentException e) {
                exchange.sendResponseHeaders(400, -1);
            } catch (Exception e) {
                log.error("Unexpected error on {}", selfHttpEndpoint, e);
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
     * Проксирует запрос на целевую ноду через gRPC и транслирует ответ клиенту.
     *
     * @param exchange       входящий HTTP-обмен
     * @param id             ключ
     * @param targetEndpoint HTTP-endpoint целевой ноды (ключ для поиска gRPC-клиента)
     */
    private void handleViaGrpc(HttpExchange exchange, String id, String targetEndpoint) throws IOException {
        InternalGrpcKVClient client = grpcClients.get(targetEndpoint);
        if (client == null) {
            log.error("No gRPC client for endpoint={}", targetEndpoint);
            exchange.sendResponseHeaders(500, -1);
            return;
        }

        switch (exchange.getRequestMethod()) {
            case METHOD_GET -> grpcGet(exchange, id, client);
            case METHOD_PUT -> grpcPut(exchange, id, client);
            case METHOD_DELETE -> grpcDelete(exchange, id, client);
            default -> exchange.sendResponseHeaders(405, -1);
        }
    }

    private void grpcGet(HttpExchange exchange, String id, InternalGrpcKVClient client) throws IOException {
        try {
            byte[] value = client.get(id);
            exchange.sendResponseHeaders(200, value.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(value);
            }
        } catch (NoSuchElementException e) {
            exchange.sendResponseHeaders(404, -1);
        }
    }

    private void grpcPut(HttpExchange exchange, String id, InternalGrpcKVClient client) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        client.upsert(id, body);
        exchange.sendResponseHeaders(201, -1);
    }

    private void grpcDelete(HttpExchange exchange, String id, InternalGrpcKVClient client) throws IOException {
        client.delete(id);
        exchange.sendResponseHeaders(202, -1);
    }
}
