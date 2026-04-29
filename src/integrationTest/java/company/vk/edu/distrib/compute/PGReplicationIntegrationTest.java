package company.vk.edu.distrib.compute;

import company.vk.edu.distrib.compute.golubtsov_pavel.PGReplicatedServiceFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.net.http.HttpResponse;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PGReplicationIntegrationTest extends TestBase {
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
    private final KVServiceFactory serviceFactory = new PGReplicatedServiceFactory();

    @AfterAll
    static void afterAll() {
        HTTP_CLIENT.close();
    }

    @Test
    void serviceSupportsReplication() throws Exception {
        assertInstanceOf(ReplicatedService.class, serviceFactory.create(randomPort()));
    }

    @Test
    void replicationScenarios() {
        assertTimeoutPreemptively(TIMEOUT, () -> {
            ReplicatedService service = (ReplicatedService) serviceFactory.create(randomPort());
            service.start();
            try {
                int n = service.numberOfReplicas();
                assertTrue(n > 2);

                assertEquals(400, get(endpoint(service.port()), "bad", n + 1).statusCode());

                assertEquals(201, upsert(endpoint(service.port()), "k1", "v1".getBytes(UTF_8)).statusCode());
                assertEquals("v1", new String(get(endpoint(service.port()), "k1").body(), UTF_8));

                int w = n - 1;
                int r = n - w + 1;
                assertEquals(201, upsert(endpoint(service.port()), "k2", "v2".getBytes(UTF_8), w).statusCode());
                assertEquals("v2", new String(get(endpoint(service.port()), "k2", r).body(), UTF_8));

                service.disableReplica(0);
                assertEquals(201, upsert(endpoint(service.port()), "k3", "v3".getBytes(UTF_8), w).statusCode());
                service.disableReplica(1);
                assertEquals("v3", new String(get(endpoint(service.port()), "k3", n - 2).body(), UTF_8));

                service.enableReplica(0);
                service.enableReplica(1);

                assertEquals(201, upsert(endpoint(service.port()), "k4", "v4_0".getBytes(UTF_8), n).statusCode());
                service.disableReplica(0);
                service.disableReplica(1);
                assertEquals(201, upsert(endpoint(service.port()), "k4", "v4_1".getBytes(UTF_8), n - 2).statusCode());
                service.enableReplica(0);
                service.enableReplica(1);
                assertEquals("v4_1", new String(get(endpoint(service.port()), "k4", n).body(), UTF_8));

                assertEquals(201, upsert(endpoint(service.port()), "k5", "v5".getBytes(UTF_8), n).statusCode());
                service.disableReplica(1);
                service.disableReplica(2);
                assertEquals(202, delete(endpoint(service.port()), "k5", n - 2).statusCode());
                service.enableReplica(1);
                service.enableReplica(2);
                HttpResponse<byte[]> deleted = get(endpoint(service.port()), "k5", n);
                assertEquals(404, deleted.statusCode());
            } finally {
                service.stop();
            }
        });
    }

    @Override
    protected HttpClient getHttpClient() {
        return HTTP_CLIENT;
    }
}
