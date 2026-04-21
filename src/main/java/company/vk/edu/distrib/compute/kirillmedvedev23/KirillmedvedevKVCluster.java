package company.vk.edu.distrib.compute.kirillmedvedev23;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVCluster;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

@SuppressFBWarnings(
        value = {"REC_CCC_EXCEPTION_NOT_THROWN", "DM_BOXED_PRIMITIVE_FOR_PARSING", "UMAC_UNCALLED_METHOD"},
        justification = "Required for hash computation and HTTP handling")
@SuppressWarnings("PMD.CouplingBetweenObjects")
public class KirillmedvedevKVCluster implements KVCluster {
    private static final Logger log = LoggerFactory.getLogger(KirillmedvedevKVCluster.class);
    private static final int VIRTUAL_NODES = 150;
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    private final List<Integer> ports;
    private final Map<Integer, HttpServer> servers = new ConcurrentHashMap<>();
    private final Map<Integer, Dao<byte[]>> daos = new ConcurrentHashMap<>();
    private final ConsistentHashingRing ring;
    private final ReentrantLock lock = new ReentrantLock();

    public KirillmedvedevKVCluster(List<Integer> ports) {
        this.ports = new ArrayList<>(ports);
        this.ring = new ConsistentHashingRing(this.ports);
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
            throw new RuntimeException("Failed to start node on port " + port, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Failed to start node on port " + port, e);
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
                if (response.statusCode() == 200) {
                    return;
                }
            } catch (Exception e) {
                // Server not ready yet
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

    private final class StatusHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) {
            try {
                if ("GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(200, -1);
                } else {
                    exchange.sendResponseHeaders(405, -1);
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

                int targetPort = getTargetPort(id);
                String targetEndpoint = "http://localhost:" + targetPort;

                if (targetPort == localPort) {
                    handleLocal(exchange, id);
                } else {
                    proxyRequest(exchange, targetEndpoint, id);
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

        private void handleLocal(HttpExchange exchange, String id) throws IOException {
            String method = exchange.getRequestMethod();
            switch (method) {
                case "GET" -> handleGet(exchange, id);
                case "PUT" -> handlePut(exchange, id);
                case "DELETE" -> handleDelete(exchange, id);
                default -> exchange.sendResponseHeaders(405, -1);
            }
        }

        private void handleGet(HttpExchange exchange, String id) throws IOException {
            byte[] value = localDao.get(id);
            exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
            exchange.sendResponseHeaders(200, value.length);
            OutputStream os = exchange.getResponseBody();
            os.write(value);
            os.close();
        }

        private void handlePut(HttpExchange exchange, String id) throws IOException {
            InputStream is = exchange.getRequestBody();
            byte[] body = is.readAllBytes();
            localDao.upsert(id, body);
            exchange.sendResponseHeaders(201, -1);
        }

        private void handleDelete(HttpExchange exchange, String id) throws IOException {
            localDao.delete(id);
            exchange.sendResponseHeaders(202, -1);
        }

        private void proxyRequest(HttpExchange exchange, String targetEndpoint, String id) throws IOException {
            String url = targetEndpoint + "/v0/entity?id=" + id;

            byte[] body = new byte[0];
            if (!"DELETE".equals(exchange.getRequestMethod())) {
                InputStream is = exchange.getRequestBody();
                body = is.readAllBytes();
            }

            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .method(exchange.getRequestMethod(), HttpRequest.BodyPublishers.ofByteArray(body));

            String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
            if (contentType != null) {
                requestBuilder.header("Content-Type", contentType);
            }

            HttpResponse<byte[]> response;
            try {
                response = HTTP_CLIENT.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofByteArray());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                exchange.sendResponseHeaders(500, -1);
                return;
            } catch (Exception e) {
                log.error("Proxy request failed", e);
                exchange.sendResponseHeaders(404, -1);
                return;
            }

            for (String name : response.headers().map().keySet()) {
                exchange.getResponseHeaders().add(name, String.join(",", response.headers().allValues(name)));
            }
            exchange.sendResponseHeaders(response.statusCode(), response.body().length);
            OutputStream os = exchange.getResponseBody();
            os.write(response.body());
            os.close();
        }

        private String extractId(URI uri) {
            String query = uri.getRawQuery();
            if (query == null || query.isEmpty()) {
                return null;
            }
            for (String param : query.split("&")) {
                String[] kv = param.split("=", 2);
                if (kv.length == 2 && "id".equals(kv[0])) {
                    return kv[1];
                }
            }
            return null;
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
