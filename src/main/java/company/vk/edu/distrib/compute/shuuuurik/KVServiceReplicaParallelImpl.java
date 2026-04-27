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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.atomic.AtomicBoolean;

import static company.vk.edu.distrib.compute.shuuuurik.util.HttpUtils.parseQueryParams;

/**
 * HTTP-сервер с кластером файловых узлов, репликацией без ведущих узлов (запись и чтение по кворуму)
 * и параллельным I/O.
 *
 * <p>Отличие от {@link KVServiceReplicaImpl}: все N операций с репликами запускаются
 * одновременно через {@link ExecutorCompletionService}, а не последовательно.
 * При достижении ack подтверждений результат возвращается клиенту немедленно,
 * не дожидаясь завершения оставшихся задач.
 *
 * <p>Архитектура:
 * <ul>
 *   <li>Кластер содержит {@code totalNodes} узлов ({@link ReplicaNode})</li>
 *   <li>Фактор репликации N: каждый ключ хранится на N узлах из totalNodes</li>
 *   <li>Параллельный I/O: все N операций запускаются одновременно через {@link ExecutorCompletionService}</li>
 * </ul>
 */
@SuppressWarnings("PMD.GodClass")
public class KVServiceReplicaParallelImpl implements ReplicatedService {

    private static final Logger log = LoggerFactory.getLogger(KVServiceReplicaParallelImpl.class);

    private static final String METHOD_GET = "GET";
    private static final String METHOD_PUT = "PUT";
    private static final String METHOD_DELETE = "DELETE";

    private static final String PATH_STATUS = "/v0/status";
    private static final String PATH_ENTITY = "/v0/entity";
    private static final String PATH_STATS_REPLICA = "/stats/replica/";

    /**
     * HTTP-статус при недостаточном количестве подтверждений реплик.
     */
    private static final int STATUS_INSUFFICIENT_REPLICAS = 500;

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

    private final int replicationFactor;

    /**
     * Пул потоков для параллельных I/O операций с репликами.
     * Размер пула = количество узлов, чтобы все N операций шли параллельно.
     */
    private final ReplicaParallelExecutor parallelExecutor;

    /**
     * Обработчик эндпоинтов статистики реплик.
     */
    private final ReplicaStatsHandler statsHandler;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private HttpServer server;

    /**
     * Создаёт HTTP-сервер с поддержкой репликации и параллельным I/O.
     *
     * @param serverPort порт HTTP-сервера
     * @param nodes      все узлы кластера
     * @param router     алгоритм выбора N реплик для ключа
     * @throws IllegalArgumentException если null/пуст или replicationFactor > nodes.length
     */
    public KVServiceReplicaParallelImpl(int serverPort, ReplicaNode[] nodes, ReplicaRouter router) {
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
        this.parallelExecutor = new ReplicaParallelExecutor();
        this.statsHandler = new ReplicaStatsHandler(this.nodes, this.totalNodes);
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
            newServer.createContext(PATH_STATS_REPLICA, statsHandler::handle);
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
        parallelExecutor.shutdown();
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
     * Параллельно читает с N реплик в детерминированном порядке, выбирает запись с максимальным timestamp.
     * Tombstone с максимальным timestamp -> 404 (данные удалены). Останавливается при ack ответах (early exit).
     *
     * <p>Параллелизм: все N задач запускаются одновременно через {@link ExecutorCompletionService}.
     * Как только собрано ack успешных ответов - отправляем результат клиенту, не дожидаясь оставшихся задач.
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
        List<Callable<Optional<VersionedEntry>>> tasks = buildReadTasks(id, replicaIndices);
        Optional<List<VersionedEntry>> result = parallelExecutor.collectSuccesses(tasks, ack, id, "GET");

        if (result.isEmpty()) {
            // Недостаточно подтверждений
            exchange.sendResponseHeaders(STATUS_INSUFFICIENT_REPLICAS, -1);
            return;
        }

        List<VersionedEntry> responses = result.get();

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
     * Строит список задач чтения для каждой реплики.
     *
     * @param id             ключ
     * @param replicaIndices индексы реплик
     * @return список задач
     */
    private List<Callable<Optional<VersionedEntry>>> buildReadTasks(String id, List<Integer> replicaIndices) {
        List<Callable<Optional<VersionedEntry>>> tasks = new ArrayList<>();
        for (int idx : replicaIndices) {
            final int nodeIdx = idx;
            tasks.add(() -> {
                Optional<VersionedEntry> result = nodes[nodeIdx].read(id);
                if (log.isDebugEnabled()) {
                    log.debug("GET key={}: node-{} responded (found={})", id, nodeIdx, result.isPresent());
                }
                return result;
            });
        }
        return tasks;
    }

    /**
     * Параллельно записывает VersionedEntry с текущим timestamp на N реплик в детерминированном порядке.
     * Ждёт ack подтверждений.
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

        List<Callable<Optional<VersionedEntry>>> tasks = buildWriteTasks(id, entry, replicaIndices, "PUT");
        Optional<List<VersionedEntry>> successes = parallelExecutor.collectSuccesses(tasks, ack, id, "PUT");

        exchange.sendResponseHeaders(successes.isPresent() ? 201 : STATUS_INSUFFICIENT_REPLICAS, -1);
    }

    /**
     * Параллельно записывает tombstone с текущим timestamp на N реплик в детерминированном порядке.
     * Ждёт ack подтверждений.
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

        List<Callable<Optional<VersionedEntry>>> tasks = buildWriteTasks(id, tombstone, replicaIndices, "DELETE");
        Optional<List<VersionedEntry>> successes = parallelExecutor.collectSuccesses(tasks, ack, id, "DELETE");

        exchange.sendResponseHeaders(successes.isPresent() ? 202 : STATUS_INSUFFICIENT_REPLICAS, -1);
    }

    /**
     * Строит список задач записи (PUT или tombstone) для каждой реплики.
     *
     * @param id             ключ
     * @param entry          запись или tombstone
     * @param replicaIndices индексы реплик
     * @param operation      название операции для логирования
     * @return список задач
     */
    private List<Callable<Optional<VersionedEntry>>> buildWriteTasks(
            String id,
            VersionedEntry entry,
            List<Integer> replicaIndices,
            String operation
    ) {
        List<Callable<Optional<VersionedEntry>>> tasks = new ArrayList<>();
        for (int idx : replicaIndices) {
            final int nodeIdx = idx;
            tasks.add(() -> {
                nodes[nodeIdx].write(id, entry);
                log.debug("{} key={}: node-{} confirmed", operation, id, nodeIdx);
                return Optional.empty();
            });
        }
        return tasks;
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
