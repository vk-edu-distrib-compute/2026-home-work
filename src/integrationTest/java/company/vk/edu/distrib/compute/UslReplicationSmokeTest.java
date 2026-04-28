package company.vk.edu.distrib.compute;

import company.vk.edu.distrib.compute.usl.UslKVServiceFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UslReplicationSmokeTest extends TestBase {
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    @AfterAll
    static void afterAll() {
        HTTP_CLIENT.close();
    }

    @Test
    void factoryCreatesReplicatedService() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            KVService storage = new UslKVServiceFactory().create(randomPort());
            assertInstanceOf(ReplicatedService.class, storage);
            ReplicatedService replicatedService = (ReplicatedService) storage;
            assertTrue(replicatedService.numberOfReplicas() >= 3);
        });
    }

    @Test
    void readRepairRestoresStaleReplica() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            int port = randomPort();
            String endpoint = endpoint(port);
            KVService storage = new UslKVServiceFactory().create(port);
            ReplicatedService replicatedService = assertInstanceOf(ReplicatedService.class, storage);
            replicatedService.start();
            try {
                int replicaCount = replicatedService.numberOfReplicas();
                byte[] firstValue = "v1".getBytes(StandardCharsets.UTF_8);
                byte[] secondValue = "v2".getBytes(StandardCharsets.UTF_8);

                assertEquals(201, upsert(endpoint, "repair-key", firstValue, replicaCount).statusCode());

                replicatedService.disableReplica(0);
                assertEquals(201, upsert(endpoint, "repair-key", secondValue, replicaCount - 1).statusCode());
                replicatedService.enableReplica(0);

                HttpResponse<byte[]> repairedRead = get(endpoint, "repair-key", replicaCount);
                assertEquals(200, repairedRead.statusCode());
                assertArrayEquals(secondValue, repairedRead.body());

                for (int replicaId = 1; replicaId < replicaCount; replicaId++) {
                    replicatedService.disableReplica(replicaId);
                }

                HttpResponse<byte[]> repairedReplicaRead = get(endpoint, "repair-key", 1);
                assertEquals(200, repairedReplicaRead.statusCode());
                assertArrayEquals(secondValue, repairedReplicaRead.body());
            } finally {
                replicatedService.stop();
            }
        });
    }

    @Test
    void tombstoneWinsAfterRepair() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            int port = randomPort();
            String endpoint = endpoint(port);
            KVService storage = new UslKVServiceFactory().create(port);
            ReplicatedService replicatedService = assertInstanceOf(ReplicatedService.class, storage);
            replicatedService.start();
            try {
                int replicaCount = replicatedService.numberOfReplicas();
                assertEquals(
                    201,
                    upsert(endpoint, "deleted-key", "value".getBytes(StandardCharsets.UTF_8), replicaCount)
                        .statusCode()
                );

                replicatedService.disableReplica(0);
                assertEquals(202, delete(endpoint, "deleted-key", replicaCount - 1).statusCode());
                replicatedService.enableReplica(0);

                HttpResponse<byte[]> deletedRead = get(endpoint, "deleted-key", replicaCount);
                assertEquals(404, deletedRead.statusCode());

                for (int replicaId = 1; replicaId < replicaCount; replicaId++) {
                    replicatedService.disableReplica(replicaId);
                }

                assertEquals(404, get(endpoint, "deleted-key", 1).statusCode());
            } finally {
                replicatedService.stop();
            }
        });
    }

    @Test
    void statsEndpointReturnsAccessCounters() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            int port = randomPort();
            String endpoint = endpoint(port);
            KVService storage = new UslKVServiceFactory().create(port);
            ReplicatedService replicatedService = assertInstanceOf(ReplicatedService.class, storage);
            replicatedService.start();
            try {
                assertEquals(201, upsert(endpoint, "stats-key", new byte[]{1, 2, 3}).statusCode());
                assertEquals(200, get(endpoint, "stats-key").statusCode());
                assertEquals(202, delete(endpoint, "stats-key").statusCode());

                HttpRequest request = HttpRequest.newBuilder()
                    .GET()
                    .uri(URI.create(endpoint + "/stats/replica/0/access"))
                    .timeout(TIMEOUT)
                    .build();
                HttpResponse<byte[]> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofByteArray());
                String body = new String(response.body(), StandardCharsets.UTF_8);

                assertEquals(200, response.statusCode());
                assertTrue(body.contains("\"reads\":"));
                assertTrue(body.contains("\"writes\":"));
                assertTrue(body.contains("\"deletes\":"));
            } finally {
                replicatedService.stop();
            }
        });
    }

    @Override
    protected HttpClient getHttpClient() {
        return HTTP_CLIENT;
    }
}
