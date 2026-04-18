package company.vk.edu.distrib.compute.vitos23.shard;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.vitos23.EntityRequestProcessor;
import company.vk.edu.distrib.compute.vitos23.exception.AcknowledgementException;
import company.vk.edu.distrib.compute.vitos23.exception.NoReplicaAvailableException;
import company.vk.edu.distrib.compute.vitos23.util.ByteArrayKey;
import company.vk.edu.distrib.compute.vitos23.util.HttpCodes;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static company.vk.edu.distrib.compute.vitos23.util.HttpUtils.NO_BODY_RESPONSE_LENGTH;
import static company.vk.edu.distrib.compute.vitos23.util.HttpUtils.sendArray;
import static company.vk.edu.distrib.compute.vitos23.util.ParseUtils.parseInteger;

/// [EntityRequestProcessor] implementation for a replicated sharded cluster.
/// Sends requests to all N replica nodes and waits for >= ack confirmations.
/// For requests to replicas, direct requests are used, which are executed directly on the node that was queried.
public class ShardedEntityRequestProcessor implements EntityRequestProcessor {

    private static final int DEFAULT_ACK = 1;

    private final String currentEndpoint;
    private final ReplicaRequestExecutor replicaRequestExecutor;
    private final EntityRequestProcessor directEntityRequestProcessor;
    private final Dao<byte[]> localDao;
    private final HttpClient httpClient;
    private final int replicationFactor;

    public ShardedEntityRequestProcessor(
            String currentEndpoint,
            ReplicaRequestExecutor replicaRequestExecutor,
            EntityRequestProcessor directEntityRequestProcessor,
            Dao<byte[]> localDao,
            HttpClient httpClient,
            int replicationFactor
    ) {
        this.currentEndpoint = currentEndpoint;
        this.replicaRequestExecutor = replicaRequestExecutor;
        this.directEntityRequestProcessor = directEntityRequestProcessor;
        this.localDao = localDao;
        this.httpClient = httpClient;
        this.replicationFactor = replicationFactor;
    }

    @Override
    public void handleGet(HttpExchange exchange, String id, Map<String, String> queryParams) throws IOException {
        if (isDirectRequest(queryParams)) {
            directEntityRequestProcessor.handleGet(exchange, id, queryParams);
            return;
        }

        int expectedAck = getExpectedAck(queryParams);

        List<CompletableFuture<ReplicaResult<byte[]>>> futures =
                replicaRequestExecutor.executeOnReplicas(id, endpoint -> getFromReplica(id, endpoint));

        byte[] aggregatedResult = aggregateGetResults(futures, expectedAck);
        if (aggregatedResult == null) {
            throw new NoSuchElementException();
        }
        sendArray(exchange, aggregatedResult);
    }

    private ReplicaResult<byte[]> getFromReplica(String id, String endpoint) throws IOException, InterruptedException {
        if (endpoint.equals(currentEndpoint)) {
            try {
                return new ReplicaResult<>(true, localDao.get(id));
            } catch (NoSuchElementException e) {
                return ReplicaResult.empty();
            }
        }

        HttpRequest request = HttpRequest.newBuilder()
                .uri(getDirectUriForNode(id, endpoint))
                .GET()
                .build();
        HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() == HttpCodes.NOT_FOUND) {
            return ReplicaResult.empty();
        }
        return new ReplicaResult<>(response.statusCode() == HttpCodes.OK, response.body());
    }

    /// Resolve aggregated GET result.
    /// Since no timestamp is associated with the key, we simply use the most frequent value
    /// returned by at least `expectedAck` nodes.
    private byte[] aggregateGetResults(
            List<CompletableFuture<ReplicaResult<byte[]>>> futures,
            int expectedAck
    ) {
        Map<ByteArrayKey, Long> frequencyByData = futures.stream()
                .map(CompletableFuture::join)
                .filter(ReplicaResult::success)
                .map(result -> new ByteArrayKey(result.data()))
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        if (frequencyByData.isEmpty()) {
            throw new NoReplicaAvailableException();
        }

        Map.Entry<ByteArrayKey, Long> mostFrequentEntry = frequencyByData.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .orElseThrow();
        if (mostFrequentEntry.getValue() < expectedAck) {
            throw new AcknowledgementException();
        }
        return mostFrequentEntry.getKey().data();
    }

    @Override
    public void handlePut(HttpExchange exchange, String id, Map<String, String> queryParams) throws IOException {
        if (isDirectRequest(queryParams)) {
            directEntityRequestProcessor.handlePut(exchange, id, queryParams);
            return;
        }

        int expectedAck = getExpectedAck(queryParams);
        byte[] body = exchange.getRequestBody().readAllBytes();
        List<CompletableFuture<ReplicaResult<Boolean>>> futures =
                replicaRequestExecutor.executeOnReplicas(id, endpoint -> putOnReplica(id, body, endpoint));
        processUpdateActionReplicaResults(futures, expectedAck, exchange, HttpCodes.CREATED);
    }

    private ReplicaResult<Boolean> putOnReplica(
            String id,
            byte[] body,
            String endpoint
    ) throws IOException, InterruptedException {
        if (endpoint.equals(currentEndpoint)) {
            try {
                localDao.upsert(id, body);
                return ReplicaResult.empty();
            } catch (Exception e) {
                return ReplicaResult.failed();
            }
        }
        HttpRequest request = HttpRequest.newBuilder()
                .uri(getDirectUriForNode(id, endpoint))
                .PUT(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
        HttpResponse<?> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
        return new ReplicaResult<>(response.statusCode() == HttpCodes.CREATED, null);
    }

    @Override
    public void handleDelete(HttpExchange exchange, String id, Map<String, String> queryParams) throws IOException {
        if (isDirectRequest(queryParams)) {
            directEntityRequestProcessor.handleDelete(exchange, id, queryParams);
            return;
        }

        int expectedAck = getExpectedAck(queryParams);
        List<CompletableFuture<ReplicaResult<Boolean>>> futures =
                replicaRequestExecutor.executeOnReplicas(id, endpoint -> deleteOnReplica(id, endpoint));
        processUpdateActionReplicaResults(futures, expectedAck, exchange, HttpCodes.ACCEPTED);
    }

    private ReplicaResult<Boolean> deleteOnReplica(
            String id,
            String endpoint
    ) throws IOException, InterruptedException {
        if (endpoint.equals(currentEndpoint)) {
            try {
                localDao.delete(id);
                return ReplicaResult.empty();
            } catch (Exception e) {
                return ReplicaResult.failed();
            }
        }
        HttpRequest request = HttpRequest.newBuilder()
                .uri(getDirectUriForNode(id, endpoint))
                .DELETE()
                .build();
        HttpResponse<?> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
        return new ReplicaResult<>(response.statusCode() == HttpCodes.ACCEPTED, null);
    }

    /// This method awaits only first `expectedAck` successful requests.
    private void processUpdateActionReplicaResults(
            List<CompletableFuture<ReplicaResult<Boolean>>> futures,
            int expectedAck,
            HttpExchange exchange,
            int successfulStatusCode
    ) throws IOException {
        int successCount = 0;
        for (CompletableFuture<ReplicaResult<Boolean>> future : futures) {
            if (future.join().success()) {
                successCount++;
                if (successCount >= expectedAck) {
                    exchange.sendResponseHeaders(successfulStatusCode, NO_BODY_RESPONSE_LENGTH);
                    return;
                }
            }
        }
        throw new AcknowledgementException();
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

    private static boolean isDirectRequest(Map<String, String> queryParams) {
        return "true".equals(queryParams.get("direct"));
    }

    private static URI getDirectUriForNode(String id, String newEndpoint) {
        return URI.create(newEndpoint + "/v0/entity?direct=true&id=" + id);
    }

}
