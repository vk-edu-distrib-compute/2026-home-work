package company.vk.edu.distrib.compute.maryarta.replication;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.ReplicatedService;
import company.vk.edu.distrib.compute.maryarta.H2Dao;
import company.vk.edu.distrib.compute.maryarta.sharding.ShardingStrategy;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ReplicatedServiceImpl implements ReplicatedService {
    private static final String INTERNAL_REPLICATION_HEADER = "X-Internal-Replication";
    private HttpServer server;
    private final H2Dao dao;
    private final int port;
    private boolean started;
//    private final ShardingStrategy shardingStrategy;
    private ExecutorService executor;
    private final ReplicationCoordinator replicationCoordinator;


    public ReplicatedServiceImpl(int port, ShardingStrategy shardingStrategy, int replicationFactor, List<String> endpoints) throws IOException {
        this.port = port;
        this.dao = new H2Dao("node-" + port);
        String selfEndpoint = "http://localhost:" + port;
//        this.shardingStrategy = shardingStrategy;
        HttpClient client = HttpClient.newHttpClient();
        this.replicationCoordinator = new ReplicationCoordinator(endpoints, replicationFactor, shardingStrategy, client, selfEndpoint, (H2Dao) dao);
    }

    @Override
    public void start() {
        if (started) {
            return;
        }
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
            executor = Executors.newVirtualThreadPerTaskExecutor();
            server.setExecutor(executor);
            createContext();
            server.start();
            started = true;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to start server on port " + port, e);
        }
    }

    @Override
    public void stop() {
        if (!started) {
            return;
        }
        server.stop(0);
        if (executor != null) {
            executor.close();
        }
        started = false;
    }

    private void createContext() {
        server.createContext("/v0/status", handleStatusRequest());
        server.createContext("/v0/entity", handleEntityRequest());
    }

    private HttpHandler handleStatusRequest() {
        return exchange -> {
            String method = exchange.getRequestMethod();
            if ("GET".equals(method)) {
                exchange.sendResponseHeaders(200, -1);
            } else {
                exchange.sendResponseHeaders(405, -1);
            }
            exchange.close();
        };
    }

    private HttpHandler handleEntityRequest() {
        return exchange -> {
            try (exchange) {
                try {
                    String method = exchange.getRequestMethod();
                    String query = exchange.getRequestURI().getQuery();
                    String id = parseId(query);
                    if ("true".equals(exchange.getRequestHeaders().getFirst(INTERNAL_REPLICATION_HEADER))) {
                        handleInternalReplicaRequest(exchange, method, id);
                        return;
                    }
                    int ack = parseAck(query);
                    switch (method) {
                        case "GET" -> {
                            byte[] value = replicationCoordinator.get(ack, id);
                            exchange.sendResponseHeaders(200, value.length);
                            exchange.getResponseBody().write(value);
                        }
                        case "PUT" -> {
                            byte[] value = exchange.getRequestBody().readAllBytes();
                            replicationCoordinator.upsert(ack, id, value);
                            exchange.sendResponseHeaders(201, -1);
                        }
                        case "DELETE" -> {
                            replicationCoordinator.delete(ack, id);
                            exchange.sendResponseHeaders(202, -1);
                        }
                        default -> exchange.sendResponseHeaders(405, -1);
                    }
                } catch (IllegalArgumentException e) {
                    exchange.sendResponseHeaders(400, -1);
                } catch (NoSuchElementException e) {
                    exchange.sendResponseHeaders(404, -1);
                } catch (IllegalStateException | IOException e) {
                    exchange.sendResponseHeaders(500, -1);
                }
            }
        };
    }

    void handleInternalReplicaRequest(HttpExchange exchange, String method, String id) throws IOException {
        switch (method) {
            case "PUT" -> {
                StoredRecord record = readStoredRecord(exchange);
                if (record.isDeleted()) {
                    throw new IllegalArgumentException("PUT request must not contain deleted record");
                }
                dao.upsert(id, record.getData(), record.getVersion(), false);
                exchange.sendResponseHeaders(201, -1);
            }
            case "DELETE" -> {
                StoredRecord record = readStoredRecord(exchange);
                if (!record.isDeleted()) {
                    throw new IllegalArgumentException("DELETE request must contain tombstone");
                }
                dao.delete(id, record.getVersion());
                exchange.sendResponseHeaders(202, -1);
            }
            case "GET" -> {
                StoredRecord record = dao.getRecord(id);
                if (record == null) {
                    exchange.sendResponseHeaders(404, -1);
                    return;
                }
                byte[] body = writeStoredRecord(record);
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
            }
            default -> exchange.sendResponseHeaders(405, -1);
        }
    }

    private byte[] writeStoredRecord(StoredRecord record) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

        try (DataOutputStream out = new DataOutputStream(byteStream)) {
            out.writeLong(record.getVersion());
            out.writeBoolean(record.isDeleted());

            byte[] data = record.getData();

            if (data == null) {
                out.writeInt(-1);
            } else {
                out.writeInt(data.length);
                out.write(data);
            }
        }

        return byteStream.toByteArray();
    }

    private StoredRecord readStoredRecord(HttpExchange exchange) throws IOException {
        byte[] requestBody = exchange.getRequestBody().readAllBytes();

        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(requestBody))) {
            long version = in.readLong();
            boolean deleted = in.readBoolean();
            int dataLength = in.readInt();

            byte[] data = null;

            if (dataLength >= 0) {
                data = in.readNBytes(dataLength);

                if (data.length != dataLength) {
                    throw new IOException("Corrupted stored record body");
                }
            }

            return new StoredRecord(data, version, deleted);
        }
    }


    private static String parseId(String query) {
        if (query != null && query.startsWith("id=")) {
            return query.substring(3);
        } else {
            throw new IllegalArgumentException("Bad query");
        }
    }

    private static int parseAck(String query){
        if (query != null && query.startsWith("ack=")) {
            return Integer.parseInt(query.substring(4));
        } else {
             return 1;
//            throw new IllegalArgumentException("Bad query");
        }
    }

    @Override
    public int port() {
        return port;
    }

    @Override
    public int numberOfReplicas() {
        return replicationCoordinator.numberOfReplicas();
    }

    @Override
    public void disableReplica(int nodeId) {
        replicationCoordinator.disableReplica(nodeId);
    }

    @Override
    public void enableReplica(int nodeId) {
        replicationCoordinator.enableReplica(nodeId);
    }
}