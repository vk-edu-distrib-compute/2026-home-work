package company.vk.edu.distrib.compute.vitos23.shard;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.vitos23.EntityRequestProcessor;
import company.vk.edu.distrib.compute.vitos23.exception.AcknowledgementException;
import company.vk.edu.distrib.compute.vitos23.util.ByteArrayKey;
import company.vk.edu.distrib.compute.vitos23.util.HttpCodes;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static company.vk.edu.distrib.compute.vitos23.util.HttpUtils.NO_BODY_RESPONSE_LENGTH;
import static company.vk.edu.distrib.compute.vitos23.util.HttpUtils.sendArray;
import static company.vk.edu.distrib.compute.vitos23.util.ParseUtils.parseInteger;

/// [EntityRequestProcessor] implementation for a replicated sharded cluster.
/// Sends requests to all N replica nodes and waits for >= ack confirmations.
/// For requests to replicas, direct requests are used, which are executed directly on the node that was queried.
public class ShardedEntityRequestProcessor implements EntityRequestProcessor {

    private static final int DEFAULT_ACK = 1;
    private static final Duration TIMEOUT = Duration.ofSeconds(3);

    private final ReplicaRequestExecutor replicaRequestExecutor;
    private final int replicationFactor;

    public ShardedEntityRequestProcessor(
            ReplicaRequestExecutor replicaRequestExecutor,
            int replicationFactor
    ) {
        this.replicaRequestExecutor = replicaRequestExecutor;
        this.replicationFactor = replicationFactor;
    }

    @Override
    public void handleGet(
            HttpExchange exchange,
            String id,
            Map<String, String> queryParams
    ) throws IOException {
        int expectedAck = getExpectedAck(queryParams);

        List<Mono<ByteArrayKey>> replicaResults = replicaRequestExecutor.getFromReplicas(id);
        byte[] aggregatedResult = aggregateGetResults(replicaResults, expectedAck);
        if (aggregatedResult == null) {
            throw new NoSuchElementException();
        }
        sendArray(exchange, aggregatedResult);
    }

    /// Resolve aggregated GET result.
    /// Since no timestamp is associated with the key, we simply use the first value
    /// returned by at least `expectedAck` nodes.
    private byte[] aggregateGetResults(List<Mono<ByteArrayKey>> replicaResults, int expectedAck) {
        @SuppressWarnings("PMD.UseConcurrentHashMap") // false positive
        Map<ByteArrayKey, Integer> frequencyByData = new HashMap<>();
        for (Mono<ByteArrayKey> mono : replicaResults) {
            ByteArrayKey result;
            try {
                result = mono.block(TIMEOUT);
            } catch (RuntimeException e) {
                continue;
            }
            if (result == null) {
                continue;
            }
            // I had to disable all the warnings altogether because build's and Codacy's PMD have different settings.
            // So in Codacy the following statements produces AvoidInstantiatingObjectsInLoops warning.
            // If I suppress only it, I get UnnecessaryWarningSuppression warning in check task.
            @SuppressWarnings("all")
            int count = frequencyByData.merge(result, 1, Integer::sum);
            if (count >= expectedAck) {
                return result.data();
            }
        }
        throw new AcknowledgementException();
    }

    @Override
    public void handlePut(
            HttpExchange exchange,
            String id,
            Map<String, String> queryParams
    ) throws IOException {
        int expectedAck = getExpectedAck(queryParams);
        byte[] body = exchange.getRequestBody().readAllBytes();
        List<Mono<Void>> replicaResults = replicaRequestExecutor.upsertToReplicas(id, body);
        processUpdateActionReplicaResults(replicaResults, expectedAck, exchange, HttpCodes.CREATED);
    }

    @Override
    public void handleDelete(
            HttpExchange exchange,
            String id,
            Map<String, String> queryParams
    ) throws IOException {
        int expectedAck = getExpectedAck(queryParams);
        List<Mono<Void>> replicaResults = replicaRequestExecutor.deleteOnReplicas(id);
        processUpdateActionReplicaResults(replicaResults, expectedAck, exchange, HttpCodes.ACCEPTED);
    }

    /// This method awaits only first `expectedAck` successful requests.
    private void processUpdateActionReplicaResults(
            List<Mono<Void>> replicaResults,
            int expectedAck,
            HttpExchange exchange,
            int successfulStatusCode
    ) throws IOException {
        Long successCount;
        try {
            successCount = Flux.merge(
                            replicaResults.stream()
                                    .map(m -> m.thenReturn(true).onErrorReturn(false))
                                    .toList()
                    )
                    .filter(Boolean::booleanValue)
                    .take(expectedAck)
                    .count()
                    .block(TIMEOUT);
        } catch (RuntimeException ignored) {
            throw new AcknowledgementException();
        }

        if (successCount != null && successCount >= expectedAck) {
            exchange.sendResponseHeaders(successfulStatusCode, NO_BODY_RESPONSE_LENGTH);
        } else {
            throw new AcknowledgementException();
        }
    }

    private int getExpectedAck(Map<String, String> queryParams) {
        Integer ack = parseInteger(queryParams.get("ack"));
        int expectedAck = ack != null ? ack : DEFAULT_ACK;

        if (expectedAck <= 0) {
            throw new IllegalArgumentException("Ack must be positive number");
        }
        if (expectedAck > replicationFactor) {
            throw new IllegalArgumentException("Ack cannot exceed replication factor");
        }
        return expectedAck;
    }

    @Override
    public void close() throws Exception {
        replicaRequestExecutor.close();
    }
}
