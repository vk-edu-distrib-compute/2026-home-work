package company.vk.edu.distrib.compute;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

@ParameterizedClass
@ArgumentsSource(KVClusterFactoryArgumentsProvider.class)
class ShardingTest extends TestBase {
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
    private static final Duration TIMEOUT = Duration.ofSeconds(5);
    private static final int CLUSTER_SIZE = 2;

    @Parameter
    KVClusterFactory kvClusterFactory;

    @AfterAll
    static void afterAll() {
        HTTP_CLIENT.close();
    }

    @Test
    void insertAndReadFromAnyNode() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            KVCluster cluster = kvClusterFactory.create(generateRandomPorts());
            cluster.start();
            try {
                String key = randomKey();
                byte[] value = randomValue();

                for (String endpoint : cluster.getEndpoints()) {
                    assertEquals(201, upsert(endpoint, key, value).statusCode());
                }

                for (String endpoint : cluster.getEndpoints()) {
                    HttpResponse<byte[]> response = get(endpoint, key);
                    assertEquals(200, response.statusCode());
                    assertArrayEquals(value, response.body());
                }
            } finally {
                cluster.stop();
            }
        });
    }

    @Test
    void keepsSingleReplicaAcrossNodes() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            KVCluster cluster = kvClusterFactory.create(generateRandomPorts());
            cluster.start();
            try {
                String key = randomKey();
                byte[] value = randomValue();

                for (String endpoint : cluster.getEndpoints()) {
                    assertEquals(201, upsert(endpoint, key, value).statusCode());
                }

                cluster.stop();

                int successCount = 0;
                for (String endpoint : cluster.getEndpoints()) {
                    cluster.start(endpoint);
                    HttpResponse<byte[]> response = get(endpoint, key);
                    if (response.statusCode() == 200 && Arrays.equals(value, response.body())) {
                        successCount++;
                    }
                    cluster.stop(endpoint);
                }
                assertEquals(1, successCount);
            } finally {
                cluster.stop();
            }
        });
    }

    private static HttpResponse<byte[]> get(String endpoint, String key) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .GET()
                .uri(URI.create(endpoint + "/v0/entity?id=" + key))
                .timeout(Duration.ofSeconds(2))
                .build();
        return HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofByteArray());
    }

    private static HttpResponse<Void> upsert(String endpoint, String key, byte[] data) throws Exception {
        HttpRequest request = HttpRequest.newBuilder()
                .PUT(HttpRequest.BodyPublishers.ofByteArray(data))
                .uri(URI.create(endpoint + "/v0/entity?id=" + key))
                .timeout(Duration.ofSeconds(2))
                .build();
        return HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.discarding());
    }

    private static List<Integer> generateRandomPorts() {
        Set<Integer> ports = new HashSet<>();
        while (ports.size() < CLUSTER_SIZE) {
            ports.add(randomPort());
        }
        return new ArrayList<>(ports);
    }
}
