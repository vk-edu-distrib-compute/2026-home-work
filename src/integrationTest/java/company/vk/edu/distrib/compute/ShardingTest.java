package company.vk.edu.distrib.compute;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.net.http.HttpClient;
import java.net.http.HttpResponse;
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
    static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
    static final int CLUSTER_SIZE = 2;

    @Parameter
    KVClusterFactory kvClusterFactory;

    @AfterAll
    static void afterAll() {
        HTTP_CLIENT.close();
    }

    @Test
    void insert() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            List<Integer> ports = generateRandomPorts();
            KVCluster storage = kvClusterFactory.create(ports);
            storage.start();
            try {
                String key = randomKey();
                byte[] value = randomValue();
                List<String> endpoints = storage.getEndpoints();

                for (String endpoint : endpoints) {
                    assertEquals(201, upsert(endpoint, key, value).statusCode());

                    for (String readEndpoint : endpoints) {
                        HttpResponse<byte[]> response = get(readEndpoint, key);
                        assertEquals(200, response.statusCode());
                        assertArrayEquals(value, response.body());
                    }
                }
            } finally {
                storage.stop();
            }
        });
    }

    @Test
    void insertEmpty() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            List<Integer> ports = generateRandomPorts();
            KVCluster storage = kvClusterFactory.create(ports);
            storage.start();
            try {
                String key = randomKey();
                byte[] value = new byte[0];
                List<String> endpoints = storage.getEndpoints();

                for (String endpoint : endpoints) {
                    assertEquals(201, upsert(endpoint, key, value).statusCode());

                    for (String readEndpoint : endpoints) {
                        HttpResponse<byte[]> response = get(readEndpoint, key);
                        assertEquals(200, response.statusCode());
                        assertArrayEquals(value, response.body());
                    }
                }
            } finally {
                storage.stop();
            }
        });
    }

    @Test
    void upsert() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            List<Integer> ports = generateRandomPorts();
            KVCluster storage = kvClusterFactory.create(ports);
            storage.start();
            try {
                String key = randomKey();
                byte[] value1 = randomValue();
                byte[] value2 = randomValue();
                List<String> endpoints = storage.getEndpoints();

                assertEquals(201, upsert(endpoints.get(0), key, value1).statusCode());
                assertEquals(201, upsert(endpoints.get(1), key, value2).statusCode());

                for (String endpoint : endpoints) {
                    HttpResponse<byte[]> response = get(endpoint, key);
                    assertEquals(200, response.statusCode());
                    assertArrayEquals(value2, response.body());
                }
            } finally {
                storage.stop();
            }
        });
    }

    @Test
    void upsertEmpty() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            List<Integer> ports = generateRandomPorts();
            KVCluster storage = kvClusterFactory.create(ports);
            storage.start();
            try {
                String key = randomKey();
                byte[] value = randomValue();
                byte[] empty = new byte[0];
                List<String> endpoints = storage.getEndpoints();

                assertEquals(201, upsert(endpoints.get(0), key, value).statusCode());
                assertEquals(201, upsert(endpoints.get(1), key, empty).statusCode());

                for (String endpoint : endpoints) {
                    HttpResponse<byte[]> response = get(endpoint, key);
                    assertEquals(200, response.statusCode());
                    assertArrayEquals(empty, response.body());
                }
            } finally {
                storage.stop();
            }
        });
    }

    @Test
    void delete() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            List<Integer> ports = generateRandomPorts();
            KVCluster storage = kvClusterFactory.create(ports);
            storage.start();
            try {
                String key = randomKey();
                byte[] value = randomValue();
                List<String> endpoints = storage.getEndpoints();

                for (String endpoint : endpoints) {
                    assertEquals(201, upsert(endpoint, key, value).statusCode());
                }

                assertEquals(202, delete(endpoints.getFirst(), key).statusCode());

                for (String endpoint : endpoints) {
                    assertEquals(404, get(endpoint, key).statusCode());
                }
            } finally {
                storage.stop();
            }
        });
    }

    @Test
    void lifecycle2keys() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            List<Integer> ports = generateRandomPorts();
            KVCluster storage = kvClusterFactory.create(ports);
            storage.start();
            try {
                String key1 = randomKey();
                byte[] value1 = randomValue();
                String key2 = randomKey();
                byte[] value2 = randomValue();
                List<String> endpoints = storage.getEndpoints();

                assertEquals(201, upsert(endpoints.get(0), key1, value1).statusCode());
                for (String endpoint : endpoints) {
                    assertArrayEquals(value1, get(endpoint, key1).body());
                }

                assertEquals(201, upsert(endpoints.get(1), key2, value2).statusCode());
                for (String endpoint : endpoints) {
                    assertArrayEquals(value1, get(endpoint, key1).body());
                    assertArrayEquals(value2, get(endpoint, key2).body());
                }

                for (String endpoint : endpoints) {
                    assertEquals(202, delete(endpoint, key1).statusCode());
                }
                for (String endpoint : endpoints) {
                    assertEquals(404, get(endpoint, key1).statusCode());
                    assertArrayEquals(value2, get(endpoint, key2).body());
                }

                for (String endpoint : endpoints) {
                    assertEquals(202, delete(endpoint, key2).statusCode());
                }
                for (String endpoint : endpoints) {
                    assertEquals(404, get(endpoint, key1).statusCode());
                    assertEquals(404, get(endpoint, key2).statusCode());
                }
            } finally {
                storage.stop();
            }
        });
    }

    @Test
    void distribute() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            List<Integer> ports = generateRandomPorts();
            KVCluster storage = kvClusterFactory.create(ports);
            storage.start();
            try {
                String key = randomKey();
                byte[] value = randomValue();
                List<String> endpoints = storage.getEndpoints();

                for (String endpoint : endpoints) {
                    assertEquals(201, upsert(endpoint, key, value).statusCode());
                }

                storage.stop();

                int successCount = 0;
                for (String endpoint : endpoints) {
                    storage.start(endpoint);

                    HttpResponse<byte[]> response = get(endpoint, key);
                    if (200 == response.statusCode() && Arrays.equals(value, response.body())) {
                        successCount++;
                    }

                    storage.stop(endpoint);
                }

                assertEquals(1, successCount);
            } finally {
                storage.stop();
            }
        });
    }

    @Override
    protected HttpClient getHttpClient() {
        return HTTP_CLIENT;
    }

    private List<Integer> generateRandomPorts() {
        Set<Integer> result = new HashSet<>();
        while (result.size() != CLUSTER_SIZE) {
            result.add(randomPort());
        }

        return new ArrayList<>(result);
    }
}
