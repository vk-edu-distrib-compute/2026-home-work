package company.vk.edu.distrib.compute.kirillmedvedev23;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVCluster;
import company.vk.edu.distrib.compute.ReplicatedService;
import company.vk.edu.distrib.compute.kirillmedvedev23.exceptions.ClusterNodeException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

@SuppressFBWarnings(
        value = {"REC_CCC_EXCEPTION_NOT_THROWN", "DM_BOXED_PRIMITIVE_FOR_PARSING", "UMAC_UNCALLED_METHOD"},
        justification = "Required for hash computation and HTTP handling")
@SuppressWarnings("PMD.CouplingBetweenObjects")
public class KirillmedvedevKVCluster implements KVCluster, ReplicatedService {
    private static final Logger log = LoggerFactory.getLogger(KirillmedvedevKVCluster.class);
    private static final int VIRTUAL_NODES = 150;
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
    private static final String HTTP_METHOD_DELETE = "DELETE";
    private static final String QUERY_PARAM_ID = "id";
    private static final String QUERY_PARAM_ACK = "ack";
    private static final int HTTP_STATUS_ACCEPTED = 202;
    private static final int HTTP_STATUS_METHOD_NOT_ALLOWED = 405;
    private static final int DEFAULT_REPLICA_FACTOR = 3;

    private final List<Integer> ports;
    private final Map<Integer, HttpServer> servers = new ConcurrentHashMap<>();
    private final Map<Integer, Dao<byte[]>> daos = new ConcurrentHashMap<>();
    private final ConsistentHashingRing ring;
    private final ReentrantLock lock = new ReentrantLock();
    private final int replicaFactor;
    private final Set<Integer> disabledReplicas = ConcurrentHashMap.newKeySet();
    private final Random random = new Random();

    public KirillmedvedevKVCluster(List<Integer> ports) {
        this(ports, DEFAULT_REPLICA_FACTOR);
    }

    public KirillmedvedevKVCluster(List<Integer> ports, int replicaFactor) {
        this.ports = new ArrayList<>(ports);
        this.replicaFactor = replicaFactor;
        this.ring = new ConsistentHashingRing(this.ports);
    }

    @Override
    public int port() {
        return ports.get(0);
    }

    @Override
    public int numberOfReplicas() {
        return replicaFactor;
    }

    @Override
    public void disableReplica(int nodeId) {
        if (nodeId >= 0 && nodeId < ports.size()) {
            disabledReplicas.add(nodeId);
            log.info("Disabled replica {}", nodeId);
        }
    }

    @Override
    public void enableReplica(int nodeId) {
        disabledReplicas.remove(nodeId);
        log.info("Enabled replica {}", nodeId);
    }

    private Dao<byte[]> getDao(int portIndex) {
        Integer port = ports.get(portIndex);
        return daos.get(port);
    }

    @Override
    public void start() {
        lock.lock();
        try {
            for (Integer port : ports) {
                startNode(port);
            }
            if (log.isInfoEnabled()) {
                log.info("Cluster started with {} nodes", ports.size());
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void start(String endpoint) {
        int port = parsePort(endpoint);
        startNode(port);
    }

    private void startNode(int port) {
        lock.lock();
        try {
            if (servers.containsKey(port)) {
                return;
            }

            Path storageDir = Path.of(System.getProperty("java.io.tmpdir"), "kv-cluster-" + port);
            Files.createDirectories(storageDir);
            Dao<byte[]> dao = new KirillmedvedevFileSystemDao(storageDir);
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);

            server.createContext("/v0/status", new StatusHandler());
            server.createContext("/v0/entity", new EntityHandler(dao, port));

            server.setExecutor(Executors.newFixedThreadPool(4));
            server.start();

            waitForServer(port);

            servers.put(port, server);
            daos.put(port, dao);

            log.info("Node started on port {}", port);
        } catch (IOException e) {
            throw new ClusterNodeException("Failed to start node on port " + port, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ClusterNodeException("Failed to start node on port " + port, e);
        } finally {
            lock.unlock();
        }
    }

    private void waitForServer(int port) throws InterruptedException {
        int maxRetries = 200;
        for (int i = 0; i < maxRetries; i++) {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create("http://localhost:" + port + "/v0/status"))
                        .GET()
                        .build();
                HttpResponse<Void> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.discarding());
                if (response.statusCode() == HttpURLConnection.HTTP_OK) {
                    return;
                }
            } catch (IOException | InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            Thread.sleep(20);
        }
    }

    @Override
    public void stop() {
        lock.lock();
        try {
            for (Integer port : new ArrayList<>(servers.keySet())) {
                stopNode(port);
            }
            log.info("Cluster stopped");
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void stop(String endpoint) {
        int port = parsePort(endpoint);
        stopNode(port);
    }

    private void stopNode(int port) {
        lock.lock();
        try {
            HttpServer server = servers.remove(port);
            if (server != null) {
                server.stop(0);
            }
            Dao<byte[]> dao = daos.remove(port);
            if (dao != null) {
                try {
                    dao.close();
                } catch (IOException e) {
                    log.debug("Error closing dao", e);
                }
            }
            log.info("Node stopped on port {}", port);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<String> getEndpoints() {
        return ports.stream()
                .map(port -> "http://localhost:" + port)
                .collect(Collectors.toList());
    }

    private int parsePort(String endpoint) {
        String[] parts = endpoint.split(":");
        return Integer.parseInt(parts[parts.length - 1]);
    }

    private int getTargetPort(String key) {
        return ring.getNode(key);
    }

    private List<Integer> getReplicaPorts(String key) {
        List<Integer> replicaPorts = new ArrayList<>();
        int primaryPort = ring.getNode(key);
        int startIndex = ports.indexOf(primaryPort);

        for (int i = 0; i < replicaFactor; i++) {
            int index = (startIndex + i) % ports.size();
            replicaPorts.add(ports.get(index));
        }
        return replicaPorts;
    }

    private boolean isReplicaEnabled(int index) {
        return !disabledReplicas.contains(index);
    }

    private int getAvailableReplicaCount() {
        int count = 0;
        for (int i = 0; i < ports.size(); i++) {
            if (isReplicaEnabled(i)) {
                count++;
            }
        }
        return count;
    }

    private final class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) {
            try {
                if ("GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, -1);
                } else {
                    exchange.sendResponseHeaders(HTTP_STATUS_METHOD_NOT_ALLOWED, -1);
                }
            } catch (IOException e) {
                log.debug("Error sending status response", e);
            }
        }
    }

private final class EntityHandler implements HttpHandler {
        private final Dao<byte[]> localDao;
        private final int localPort;

        EntityHandler(Dao<byte[]> localDao, int localPort) {
            this.localDao = localDao;
            this.localPort = localPort;
        }

        @Override
        public void handle(HttpExchange exchange) {
            try {
                String id = extractId(exchange.getRequestURI());
                if (id == null || id.isEmpty()) {
                    exchange.sendResponseHeaders(400, -1);
                    return;
                }

                int ack = extractAck(exchange.getRequestURI());
                if (ack > replicaFactor) {
                    exchange.sendResponseHeaders(400, -1);
                    return;
                }

                String method = exchange.getRequestMethod();
                switch (method) {
                    case "GET" -> handleGet(exchange, id, ack);
                    case "PUT" -> handlePut(exchange, id, ack);
                    case "DELETE" -> handleDelete(exchange, id, ack);
                    default -> exchange.sendResponseHeaders(HTTP_STATUS_METHOD_NOT_ALLOWED, -1);
                }
            } catch (NoSuchElementException e) {
                try {
                    exchange.sendResponseHeaders(404, -1);
                } catch (IOException ex) {
                    log.debug("Error sending 404", ex);
                }
            } catch (Exception e) {
                log.error("Error handling request", e);
                try {
                    exchange.sendResponseHeaders(500, -1);
                } catch (IOException ex) {
                    log.debug("Error sending error response", ex);
                }
            }
        }

        private void handleGet(HttpExchange exchange, String id, int ack) throws IOException {
            List<Integer> replicaPorts = getReplicaPorts(id);

            ExecutorService executor = Executors.newFixedThreadPool(Math.min(replicaPorts.size(), 10));
            List<Future<byte[]>> futures = new ArrayList<>();

            for (int port : replicaPorts) {
                int index = ports.indexOf(port);
                if (!isReplicaEnabled(index)) {
                    continue;
                }
                final int replicaIndex = index;
                futures.add(executor.submit(() -> {
                    try {
                        Dao<byte[]> dao = getDao(replicaIndex);
                        if (dao != null) {
                            return dao.get(id);
                        }
                    } catch (Exception e) {
                        log.debug("Failed to get from replica {}", replicaIndex, e);
                    }
                    return null;
                }));
            }

            int successCount = 0;
            byte[] latestValue = null;

            for (Future<byte[]> future : futures) {
                try {
                    byte[] value = future.get(5, TimeUnit.SECONDS);
                    if (value != null) {
                        successCount++;
                        latestValue = value;
                    }
                } catch (Exception e) {
                    log.debug("Failed to get from replica", e);
                }
            }
            executor.shutdown();

            if (successCount >= ack) {
                if (latestValue != null) {
                    exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                    exchange.sendResponseHeaders(200, latestValue.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(latestValue);
                    }
                } else {
                    exchange.sendResponseHeaders(404, -1);
                }
            } else {
                exchange.sendResponseHeaders(500, -1);
            }
        }

        private void handlePut(HttpExchange exchange, String id, int ack) throws IOException {
            try (InputStream is = exchange.getRequestBody()) {
                byte[] body = is.readAllBytes();

                List<Integer> replicaPorts = getReplicaPorts(id);
                int successCount = 0;

                ExecutorService executor = Executors.newFixedThreadPool(Math.min(replicaPorts.size(), 10));
                List<Future<Boolean>> futures = new ArrayList<>();

                for (int port : replicaPorts) {
                    int index = ports.indexOf(port);
                    if (!isReplicaEnabled(index)) {
                        continue;
                    }
                    final int replicaIndex = index;
                    final byte[] data = body;
                    futures.add(executor.submit(() -> {
                        try {
                            Dao<byte[]> dao = getDao(replicaIndex);
                            if (dao != null) {
                                dao.upsert(id, data);
                                return true;
                            }
                        } catch (Exception e) {
                            log.debug("Failed to put to replica {}", replicaIndex, e);
                        }
                        return false;
                    }));
                }

                for (Future<Boolean> future : futures) {
                    try {
                        if (Boolean.TRUE.equals(future.get(5, TimeUnit.SECONDS))) {
                            successCount++;
                        }
                    } catch (Exception e) {
                        log.debug("Failed to put to replica", e);
                    }
                }
                executor.shutdown();

                if (successCount >= ack) {
                    exchange.sendResponseHeaders(HttpURLConnection.HTTP_CREATED, -1);
                } else {
                    exchange.sendResponseHeaders(500, -1);
                }
            }
        }

        private void handleDelete(HttpExchange exchange, String id, int ack) throws IOException {
            List<Integer> replicaPorts = getReplicaPorts(id);
            int successCount = 0;

            ExecutorService executor = Executors.newFixedThreadPool(Math.min(replicaPorts.size(), 10));
            List<Future<Boolean>> futures = new ArrayList<>();

            for (int port : replicaPorts) {
                int index = ports.indexOf(port);
                if (!isReplicaEnabled(index)) {
                    continue;
                }
                final int replicaIndex = index;
                futures.add(executor.submit(() -> {
                    try {
                        Dao<byte[]> dao = getDao(replicaIndex);
                        if (dao != null) {
                            dao.delete(id);
                            return true;
                        }
                    } catch (Exception e) {
                        log.debug("Failed to delete from replica {}", replicaIndex, e);
                    }
                    return false;
                }));
            }

            for (Future<Boolean> future : futures) {
                try {
                    if (Boolean.TRUE.equals(future.get(5, TimeUnit.SECONDS))) {
                        successCount++;
                    }
                } catch (Exception e) {
                    log.debug("Failed to delete from replica", e);
                }
            }
            executor.shutdown();

            if (successCount >= ack) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_ACCEPTED, -1);
            } else {
                exchange.sendResponseHeaders(500, -1);
            }
        }

        private String extractId(URI uri) {
            String query = uri.getRawQuery();
            if (query == null || query.isEmpty()) {
                return null;
            }
            for (String param : query.split("&")) {
                String[] kv = param.split("=", 2);
                if (kv.length == 2 && QUERY_PARAM_ID.equals(kv[0])) {
                    return kv[1];
                }
            }
            return null;
        }

        private int extractAck(URI uri) {
            String query = uri.getRawQuery();
            if (query == null || query.isEmpty()) {
                return 1;
            }
            for (String param : query.split("&")) {
                String[] kv = param.split("=", 2);
                if (kv.length == 2 && QUERY_PARAM_ACK.equals(kv[0])) {
                    try {
                        return Integer.parseInt(kv[1]);
                    } catch (NumberFormatException e) {
                        return 1;
                    }
                }
            }
            return 1;
        }
    }

    private static class ConsistentHashingRing {
        private final NavigableMap<Long, Integer> ring = new TreeMap<>();
        private final List<Integer> nodes;

        ConsistentHashingRing(List<Integer> nodes) {
            this.nodes = new ArrayList<>(nodes);
            buildRing();
        }

        private void buildRing() {
            ring.clear();
            for (Integer node : nodes) {
                for (int i = 0; i < VIRTUAL_NODES; i++) {
                    long hash = hash(node + "_" + i);
                    ring.put(hash, node);
                }
            }
        }

        int getNode(String key) {
            if (ring.isEmpty()) {
                throw new IllegalStateException("No nodes in ring");
            }
            long hash = hash(key);
            Map.Entry<Long, Integer> entry = ring.ceilingEntry(hash);
            if (entry == null) {
                entry = ring.firstEntry();
            }
            return entry.getValue();
        }

        private long hash(String key) {
            return MurmurHash3Util.hash(key);
        }
    }

    @SuppressFBWarnings(
            value = {"DM_BOXED_PRIMITIVE", "PRMC_PREMATURE_CLASS"},
            justification = "Hash utility implementation")
    private static final class MurmurHash3Util {
        static long hash(String key) {
            byte[] data = key.getBytes();
            final int c1 = 0xcc9e2d51;
            final int c2 = 0x1b873593;
            int len = data.length;
            int h1 = len;
            int i = 0;

            while (i + 4 <= len) {
                int k1 = (data[i] & 0xff)
                        | ((data[i + 1] & 0xff) << 8)
                        | ((data[i + 2] & 0xff) << 16)
                        | (data[i + 3] << 24);
                k1 *= c1;
                k1 = Integer.rotateLeft(k1, 15);
                k1 *= c2;
                h1 ^= k1;
                h1 = Integer.rotateLeft(h1, 13);
                h1 = h1 * 5 + 0xe6546b64;
                i += 4;
            }

            if (i < len) {
                int k1 = 0;
                for (int j = len - 1; j >= i; j--) {
                    k1 = (k1 << 8) | (data[j] & 0xff);
                }
                k1 *= c1;
                k1 = Integer.rotateLeft(k1, 15);
                k1 *= c2;
                h1 ^= k1;
            }

            h1 ^= len;
            h1 ^= (h1 >>> 16);
            h1 *= 0x85ebca6b;
            h1 ^= (h1 >>> 13);
            h1 *= 0xc2b2ae35;
            h1 ^= (h1 >>> 16);

            return h1 & 0xffffffffL;
        }
    }
}
