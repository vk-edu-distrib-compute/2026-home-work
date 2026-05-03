package company.vk.edu.distrib.compute.shuuuurik;

import com.sun.net.httpserver.HttpExchange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * HTTP-обработчик эндпоинтов статистики реплик.
 *
 * <p>Обрабатывает два пути:
 * <ul>
 *   <li>{@code GET /stats/replica/{id}} — количество ключей в конкретной реплике</li>
 *   <li>{@code GET /stats/replica/{id}/access} — счётчики read/write операций</li>
 * </ul>
 *
 * <p>Статистика хранится в памяти в {@link ReplicaNode} и не сбрасывается между запросами.
 */
public class ReplicaStatsHandler {

    private static final Logger log = LoggerFactory.getLogger(ReplicaStatsHandler.class);

    private static final String METHOD_GET = "GET";
    private static final String PATH_STATS_REPLICA = "/stats/replica/";

    private final ReplicaNode[] nodes;
    private final int totalNodes;

    /**
     * Создаёт HTTP-обработчик эндпоинтов статистики реплик.
     *
     * @param nodes      все узлы кластера (для чтения статистики)
     * @param totalNodes количество узлов (для валидации nodeId)
     */
    ReplicaStatsHandler(ReplicaNode[] nodes, int totalNodes) {
        this.nodes = nodes.clone();
        this.totalNodes = totalNodes;
    }

    /**
     * Обрабатывает запросы статистики.
     * <ul>
     *   <li>{@code GET /stats/replica/{id}} - количество ключей</li>
     *   <li>{@code GET /stats/replica/{id}/access} - счётчики read/write операций</li>
     * </ul>
     *
     * @param exchange HTTP-обмен
     */
    void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!METHOD_GET.equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            // path = /stats/replica/3  или  /stats/replica/3/access
            String path = exchange.getRequestURI().getPath();
            String suffix = path.substring(PATH_STATS_REPLICA.length()); // "3" или "3/access"
            boolean isAccessStats = suffix.endsWith("/access");
            String nodeIdStr = isAccessStats
                    ? suffix.substring(0, suffix.length() - "/access".length())
                    : suffix;

            int nodeId;
            try {
                nodeId = Integer.parseInt(nodeIdStr);
            } catch (NumberFormatException e) {
                exchange.sendResponseHeaders(400, -1);
                return;
            }

            if (nodeId < 0 || nodeId >= totalNodes) {
                exchange.sendResponseHeaders(404, -1);
                return;
            }

            String responseBody;
            if (isAccessStats) {
                responseBody = buildAccessStatsJson(nodeId);
            } else {
                responseBody = buildReplicaStatsJson(nodeId);
            }

            byte[] body = responseBody.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        }
    }

    /**
     * Формирует JSON со статистикой данных реплики: количество ключей.
     *
     * @param nodeId номер узла
     * @return JSON-строка с полями nodeId и keyCount
     */
    private String buildReplicaStatsJson(int nodeId) {
        long keyCount = 0;
        try {
            keyCount = nodes[nodeId].countKeys();
        } catch (IOException e) {
            log.warn("Stats: failed to count keys for node-{}", nodeId, e);
        }
        return "{\"nodeId\":" + nodeId
                + ",\"keyCount\":" + keyCount + "}";
    }

    /**
     * Формирует JSON со статистикой доступа к реплике: количество операций чтения и записи.
     *
     * @param nodeId номер узла
     * @return JSON-строка с полями readCount и writeCount
     */
    private String buildAccessStatsJson(int nodeId) {
        return "{\"nodeId\":" + nodeId
                + ",\"readCount\":" + nodes[nodeId].getReadCount()
                + ",\"writeCount\":" + nodes[nodeId].getWriteCount() + "}";
    }
}
