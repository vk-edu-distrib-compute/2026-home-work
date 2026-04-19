package company.vk.edu.distrib.compute.shuuuurik;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.shuuuurik.routing.ReplicaRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serial;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import static company.vk.edu.distrib.compute.shuuuurik.util.HttpUtils.parseQueryParams;

/**
 * HTTP-сервер с кластером файловых узлов и репликацией без ведущих узлов (запись и чтение по кворуму).
 *
 * <p>Архитектура:
 * <ul>
 *   <li>Кластер содержит {@code totalNodes} узлов (FileDao), каждый в своей директории</li>
 *   <li>Фактор репликации N: каждый ключ хранится на N узлах из totalNodes</li>
 *   <li>Множество из N узлов для ключа определяется детерминированно через
 *       {@link ReplicaRouter} (Rendezvous Hashing)</li>
 *   <li>Early exit: обход N реплик прекращается при ack подтверждениях</li>
 * </ul>
 */
public class KVServiceReplicaImpl implements KVService {

    private static final Logger log = LoggerFactory.getLogger(KVServiceReplicaImpl.class);

    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";

    private static final String PATH_STATUS = "/v0/status";
    private static final String PATH_ENTITY = "/v0/entity";

    /**
     * HTTP-статус при недостаточном количестве подтверждений реплик.
     */
    private static final int STATUS_INSUFFICIENT_REPLICAS = 504;

    /**
     * Дефолтный ack = 1 обеспечивает обратную совместимость: клиент без {@code ?ack=}
     * получает тот же ответ, что и в hw-01/hw-02.
     */
    private static final int DEFAULT_ACK = 1;

    private final int port;

    /**
     * Все узлы кластера (например 10 FileDao).
     */
    private final List<Dao<byte[]>> allNodes;
    private final int totalNodes;

    /**
     * Выбирает N реплик для ключа из allNodes.
     */
    private final ReplicaRouter router;

    /**
     * Фактор репликации N - для валидации ack.
     */
    private final int replicationFactor;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private HttpServer server;

    /**
     * Создаёт HTTP-сервер с поддержкой реплицирования.
     *
     * @param port     порт HTTP-сервера
     * @param allNodes все узлы кластера (totalNodes штук, например 10)
     * @param router   алгоритм выбора N реплик для ключа
     * @throws IllegalArgumentException если allNodes null или пуст
     */
    public KVServiceReplicaImpl(int port, List<Dao<byte[]>> allNodes, ReplicaRouter router) {
        if (allNodes == null || allNodes.isEmpty()) {
            throw new IllegalArgumentException("allNodes must not be null or empty");
        }
        if (router.getReplicationFactor() > allNodes.size()) {
            throw new IllegalArgumentException(
                    "replicationFactor (" + router.getReplicationFactor()
                            + ") > totalNodes (" + allNodes.size() + ")");
        }
        this.port = port;
        this.allNodes = List.copyOf(allNodes);
        this.totalNodes = allNodes.size();
        this.router = router;
        this.replicationFactor = router.getReplicationFactor();
    }

    @Override
    public void start() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("Service already started");
        }
        try {
            HttpServer newServer = HttpServer.create();
            newServer.createContext(PATH_STATUS, this::handleStatus);
            newServer.createContext(PATH_ENTITY, this::handleEntity);
            newServer.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 0);
            newServer.start();
            this.server = newServer;
            log.info("Started: port={}, totalNodes={}, replicationFactor={}",
                    port, totalNodes, replicationFactor);
        } catch (IOException e) {
            started.set(false);
            throw new UncheckedIOException("Failed to start server on port " + port, e);
        }
    }

    @Override
    public void stop() {
        if (!started.compareAndSet(true, false)) {
            throw new IllegalStateException("Service is not started");
        }
        server.stop(0);
        closeReplicas();
        log.info("Stopped replica service on port {}", port);
    }

    /**
     * Закрывает все реплики при остановке сервиса.
     * Ошибки при закрытии логируются, но не прерывают закрытие остальных реплик.
     */
    private void closeReplicas() {
        for (int i = 0; i < allNodes.size(); i++) {
            try {
                allNodes.get(i).close();
            } catch (IOException e) {
                log.warn("Error closing replica-{}", i, e);
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
                RequestContext ctx = buildRequestContext(exchange);
                dispatchRequest(exchange, ctx);
            } catch (IllegalArgumentException e) {
                exchange.sendResponseHeaders(400, -1);
            } catch (Exception e) {
                log.error("Unexpected error while handling request", e);
                exchange.sendResponseHeaders(500, -1);
            }
        }
    }

    private RequestContext buildRequestContext(HttpExchange exchange) {
        Map<String, String> params = parseQueryParams(exchange);
        String id = params.get("id");
        if (id == null || id.isEmpty()) {
            throw new BadRequestException();
        }

        int ack = parseAck(params);

        // Детерминированно выбираем N реплик для этого ключа
        List<Integer> replicaIndices = router.getReplicasForKey(id, totalNodes);

        return new RequestContext(id, ack, replicaIndices);
    }

    private void dispatchRequest(HttpExchange exchange, RequestContext ctx) throws IOException {
        switch (exchange.getRequestMethod()) {
            case METHOD_GET -> handleGet(exchange, ctx.id(), ctx.ack(), ctx.replicas());
            case METHOD_PUT -> handlePut(exchange, ctx.id(), ctx.ack(), ctx.replicas());
            case METHOD_DELETE -> handleDelete(exchange, ctx.id(), ctx.ack(), ctx.replicas());
            default -> exchange.sendResponseHeaders(405, -1);
        }
    }

    /**
     * Читает с N реплик в детерминированном порядке. Останавливается при ack ответах.
     * Подтверждение чтения = {@code dao.get()} завершился без {@link IOException}.
     * Если {@code dao.get()} бросил {@link NoSuchElementException} - это валидный ответ (реплика жива, ключа нет).
     *
     * @param exchange       HTTP-обмен
     * @param id             ключ
     * @param ack            требуемое число подтверждений
     * @param replicaIndices индексы реплик с которых читаем
     */
    private void handleGet(HttpExchange exchange, String id, int ack, List<Integer> replicaIndices) throws IOException {
        List<byte[]> foundValues = new ArrayList<>();
        int acksCollected = 0;

        for (int idx : replicaIndices) {
            if (acksCollected >= ack) {
                break;
            }

            try {
                byte[] value = allNodes.get(idx).get(id);
                foundValues.add(value);
                acksCollected++;
                log.debug("GET key={}: node-{} returned data", id, idx);
            } catch (NoSuchElementException e) {
                acksCollected++;
                log.debug("GET key={}: node-{} returned 404", id, idx);
            } catch (IOException e) {
                log.warn("GET key={}: node-{} unavailable", id, idx, e);
            }
        }

        if (acksCollected < ack) {
            log.warn("GET key={}: only {}/{} acks from replicas {}",
                    id, acksCollected, ack, replicaIndices);
            exchange.sendResponseHeaders(STATUS_INSUFFICIENT_REPLICAS, -1);
            return;
        }

        if (foundValues.isEmpty()) {
            // Все ответившие реплики не нашли ключ
            exchange.sendResponseHeaders(404, -1);
        } else {
            // Хотя бы одна реплика нашла данные - возвращаем их
            byte[] body = foundValues.getFirst();
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        }
    }

    /**
     * Пишет на N реплик в детерминированном порядке. Останавливается при ack подтверждениях.
     * Подтверждение записи = {@code dao.upsert()} завершился без {@link IOException}.
     *
     * @param exchange       HTTP-обмен
     * @param id             ключ
     * @param ack            требуемое число подтверждений
     * @param replicaIndices индексы реплик на которые пишем
     */
    private void handlePut(HttpExchange exchange, String id, int ack, List<Integer> replicaIndices) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        int successCount = 0;

        for (int idx : replicaIndices) {
            if (successCount >= ack) {
                break;
            }

            try {
                allNodes.get(idx).upsert(id, body);
                successCount++;
                log.debug("PUT key={}: node-{} confirmed", id, idx);
            } catch (IOException e) {
                log.warn("PUT key={}: node-{} write failed", id, idx, e);
            }
        }

        if (successCount >= ack) {
            exchange.sendResponseHeaders(201, -1);
        } else {
            log.warn("PUT key={}: only {}/{} acks (need {})", id, successCount, replicationFactor, ack);
            exchange.sendResponseHeaders(STATUS_INSUFFICIENT_REPLICAS, -1);
        }
    }

    /**
     * Удаляет на N репликах в детерминированном порядке. Останавливается при ack подтверждениях.
     * Подтверждение удаления = {@code dao.delete()} завершился без {@link IOException}.
     *
     * @param exchange       HTTP-обмен
     * @param id             ключ
     * @param ack            требуемое число подтверждений
     * @param replicaIndices индексы реплик с которых удаляем
     */
    private void handleDelete(
            HttpExchange exchange,
            String id, int ack,
            List<Integer> replicaIndices
    ) throws IOException {
        int successCount = 0;

        for (int idx : replicaIndices) {
            if (successCount >= ack) {
                break;
            }

            try {
                allNodes.get(idx).delete(id);
                successCount++;
                log.debug("DELETE key={}: node-{} confirmed", id, idx);
            } catch (IOException e) {
                log.warn("DELETE key={}: node-{} delete failed", id, idx, e);
            }
        }

        if (successCount >= ack) {
            exchange.sendResponseHeaders(202, -1);
        } else {
            log.warn("DELETE key={}: only {}/{} acks (need {})", id, successCount, replicationFactor, ack);
            exchange.sendResponseHeaders(STATUS_INSUFFICIENT_REPLICAS, -1);
        }
    }

    /**
     * Разбирает query-параметр {@code ack}. Валидация: 1 <= ack <= N (replicationFactor).
     * Отсутствие параметра -> {@value DEFAULT_ACK}.
     *
     * @param params query-параметры запроса
     * @return значение ack (>= 1)
     * @throws BadRequestException если валидация не прошла
     */
    private int parseAck(Map<String, String> params) {
        String ackStr = params.get("ack");
        if (ackStr == null || ackStr.isEmpty()) {
            return DEFAULT_ACK;
        }

        try {
            int ack = Integer.parseInt(ackStr);
            if (ack <= 0 || ack > replicationFactor) {
                throw new BadRequestException();
            }
            return ack;
        } catch (NumberFormatException e) {
            throw new BadRequestException(e);
        }
    }

    private record RequestContext(
            String id,
            int ack,
            List<Integer> replicas
    ) {
    }

    private static final class BadRequestException extends RuntimeException {
        @Serial
        private static final long serialVersionUID = 1L;

        public BadRequestException() {
            super();
        }

        public BadRequestException(Throwable cause) {
            super(cause);
        }
    }
}
