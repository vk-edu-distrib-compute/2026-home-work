package company.vk.edu.distrib.compute.shuuuurik;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.ReplicatedService;
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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static company.vk.edu.distrib.compute.shuuuurik.util.HttpUtils.parseQueryParams;

/**
 * HTTP-сервер с кластером файловых узлов и репликацией без ведущих узлов (запись и чтение по кворуму).
 *
 * <p>Архитектура:
 * <ul>
 *   <li>Кластер содержит {@code totalNodes} узлов ({@link ReplicaNode}), каждый в своей директории</li>
 *   <li>Фактор репликации N: каждый ключ хранится на N узлах из totalNodes</li>
 *   <li>Множество из N узлов для ключа определяется детерминированно через
 *       {@link ReplicaRouter} (Rendezvous Hashing)</li>
 *   <li>Каждая запись содержит timestamp для разрешения конфликтов</li>
 *   <li>DELETE записывает tombstone - маркер удаления с timestamp</li>
 *   <li>Early exit: обход N реплик прекращается при достижении ack подтверждений</li>
 * </ul>
 */
public class KVServiceReplicaImpl implements ReplicatedService {

    private static final Logger log = LoggerFactory.getLogger(KVServiceReplicaImpl.class);

    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";

    private static final String PATH_STATUS = "/v0/status";
    private static final String PATH_ENTITY = "/v0/entity";

    /**
     * HTTP-статус при недостаточном количестве подтверждений реплик.
     */
    private static final int STATUS_INSUFFICIENT_REPLICAS = 500;

    /**
     * Дефолтный ack = 1 обеспечивает обратную совместимость.
     */
    private static final int DEFAULT_ACK = 1;

    private final int serverPort;

    /**
     * Все узлы кластера (например 10 ReplicaNode).
     */
    private final ReplicaNode[] nodes;
    private final int totalNodes;

    /**
     * Выбирает N реплик для ключа из nodes.
     */
    private final ReplicaRouter router;

    /**
     * Фактор репликации N - для валидации ack.
     */
    private final int replicationFactor;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private HttpServer server;

    /**
     * Создаёт HTTP-сервер с поддержкой репликации.
     *
     * @param serverPort   порт HTTP-сервера
     * @param nodes  все узлы кластера (totalNodes штук, например 10)
     * @param router алгоритм выбора N реплик для ключа
     * @throws IllegalArgumentException если null/пуст или replicationFactor > nodes.length
     */
    public KVServiceReplicaImpl(int serverPort, ReplicaNode[] nodes, ReplicaRouter router) {
        if (nodes == null || nodes.length == 0) {
            throw new IllegalArgumentException("nodes must not be null or empty");
        }
        if (router.getReplicationFactor() > nodes.length) {
            throw new IllegalArgumentException(
                    "replicationFactor (" + router.getReplicationFactor()
                            + ") > totalNodes (" + nodes.length + ")");
        }
        this.serverPort = serverPort;
        this.nodes = nodes.clone();
        this.totalNodes = nodes.length;
        this.router = router;
        this.replicationFactor = router.getReplicationFactor();
    }

    @Override
    public int port() {
        return serverPort;
    }

    @Override
    public int numberOfReplicas() {
        return replicationFactor;
    }

    /**
     * Выключает узел с заданным номером.
     * Все последующие операции с этим узлом будут бросать IOException.
     *
     * @param nodeId номер узла (0-based)
     * @throws IllegalArgumentException если nodeId вне диапазона
     */
    @Override
    public void disableReplica(int nodeId) {
        validateNodeId(nodeId);
        nodes[nodeId].disable();
    }

    /**
     * Включает узел с заданным номером.
     *
     * @param nodeId номер узла (0-based)
     * @throws IllegalArgumentException если nodeId вне диапазона
     */
    @Override
    public void enableReplica(int nodeId) {
        validateNodeId(nodeId);
        nodes[nodeId].enable();
    }

    private void validateNodeId(int nodeId) {
        if (nodeId < 0 || nodeId >= totalNodes) {
            throw new IllegalArgumentException(
                    "nodeId must be in [0, " + (totalNodes - 1) + "], got: " + nodeId);
        }
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
            newServer.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), serverPort), 0);
            newServer.start();
            this.server = newServer;
            log.info("Started: port={}, totalNodes={}, replicationFactor={}",
                    serverPort, totalNodes, replicationFactor);
        } catch (IOException e) {
            started.set(false);
            throw new UncheckedIOException("Failed to start server on port " + serverPort, e);
        }
    }

    @Override
    public void stop() {
        if (!started.compareAndSet(true, false)) {
            throw new IllegalStateException("Service is not started");
        }
        server.stop(0);
        log.info("Stopped replica service on port {}", serverPort);
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
            } catch (BadRequestException e) {
                exchange.sendResponseHeaders(400, -1);
            } catch (Exception e) {
                log.error("Unexpected error while handling request", e);
                exchange.sendResponseHeaders(500, -1);
            }
        }
    }

    /**
     * Разбирает id, ack и выбирает N реплик детерминированно.
     *
     * @param exchange HTTP-обмен
     * @return контекст запроса
     * @throws BadRequestException если id пуст или ack невалиден
     */
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
     * Читает с N реплик в детерминированном порядке, выбирает запись с максимальным timestamp.
     * Tombstone с максимальным timestamp -> 404 (данные удалены). Останавливается при ack ответах (early exit).
     *
     * <p>Подтверждение чтения = {@code nodes[idx].read(id)} завершился без {@link IOException}.
     *
     * @param exchange       HTTP-обмен
     * @param id             ключ
     * @param ack            требуемое число подтверждений
     * @param replicaIndices индексы реплик с которых читаем
     */
    private void handleGet(
            HttpExchange exchange,
            String id,
            int ack,
            List<Integer> replicaIndices
    ) throws IOException {
        List<VersionedEntry> responses = new ArrayList<>();
        int acksCollected = 0;

        for (int idx : replicaIndices) {
            if (acksCollected >= ack) {
                break;
            }

            try {
                Optional<VersionedEntry> entry = nodes[idx].read(id);
                entry.ifPresent(responses::add);
                acksCollected++;
                if (log.isDebugEnabled()) {
                    log.debug("GET key={}: node-{} responded (found={})", id, idx, entry.isPresent());
                }
            } catch (IOException e) {
                log.warn("GET key={}: node-{} unavailable", id, idx, e);
            }
        }

        if (acksCollected < ack) {
            log.warn("GET key={}: only {}/{} acks", id, acksCollected, ack);
            exchange.sendResponseHeaders(STATUS_INSUFFICIENT_REPLICAS, -1);
            return;
        }

        if (responses.isEmpty()) {
            // Все ответившие реплики не нашли ключ
            exchange.sendResponseHeaders(404, -1);
            return;
        }

        // Выбираем запись с максимальным timestamp - разрешение конфликтов
        VersionedEntry winner = responses.stream()
                .max(Comparator.comparingLong(VersionedEntry::getTimestamp))
                .orElseThrow();

        if (winner.isTombstone()) {
            // Самая свежая операция была удалением
            exchange.sendResponseHeaders(404, -1);
        } else {
            byte[] body = winner.getValue();
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        }
    }

    /**
     * Записывает VersionedEntry с текущим timestamp на N реплик в детерминированном порядке.
     * Останавливается при ack подтверждениях.
     * Подтверждение записи = {@code nodes[idx].write()} завершился без {@link IOException}.
     *
     * @param exchange       HTTP-обмен
     * @param id             ключ
     * @param ack            требуемое число подтверждений
     * @param replicaIndices индексы реплик на которые пишем
     */
    private void handlePut(
            HttpExchange exchange,
            String id,
            int ack,
            List<Integer> replicaIndices
    ) throws IOException {
        byte[] body = exchange.getRequestBody().readAllBytes();
        VersionedEntry entry = new VersionedEntry(body, System.currentTimeMillis());

        int successCount = 0;
        for (int idx : replicaIndices) {
            if (successCount >= ack) {
                break;
            }

            try {
                nodes[idx].write(id, entry);
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
     * Записывает tombstone с текущим timestamp на N реплик в детерминированном порядке.
     * Останавливается при ack подтверждениях.
     * Подтверждение удаления = {@code nodes[idx].write(id, tombstone)} завершился без {@link IOException}.
     *
     * @param exchange       HTTP-обмен
     * @param id             ключ
     * @param ack            требуемое число подтверждений
     * @param replicaIndices индексы реплик для удаления
     */
    private void handleDelete(
            HttpExchange exchange,
            String id,
            int ack,
            List<Integer> replicaIndices
    ) throws IOException {
        VersionedEntry tombstone = new VersionedEntry(System.currentTimeMillis());

        int successCount = 0;
        for (int idx : replicaIndices) {
            if (successCount >= ack) {
                break;
            }
            try {
                nodes[idx].write(id, tombstone);
                successCount++;
                log.debug("DELETE key={}: node-{} confirmed (tombstone written)", id, idx);
            } catch (IOException e) {
                log.warn("DELETE key={}: node-{} failed", id, idx, e);
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
