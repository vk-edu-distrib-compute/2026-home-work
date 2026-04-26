package company.vk.edu.distrib.compute.denchika.service;

import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;
import com.sun.net.httpserver.*;
import company.vk.edu.distrib.compute.denchika.cluster.KVClusterProxy;
import company.vk.edu.distrib.compute.denchika.cluster.hashing.DistributingAlgorithm;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.NoSuchElementException;

public class ClusterKVService implements KVService {

    private final int port;
    private final Dao<byte[]> dao;
    private final DistributingAlgorithm hasher;
    private final List<String> nodes;

    private final String self;
    private HttpServer server;

    private final KVClusterProxy proxy = new KVClusterProxy();

    public ClusterKVService(int port,
                            Dao<byte[]> dao,
                            DistributingAlgorithm hasher,
                            List<String> nodes) {
        this.port = port;
        this.dao = dao;
        this.hasher = hasher;
        this.nodes = nodes;
        this.self = "http://localhost:" + port;
    }

    @Override
    public void start() {
        try {
            server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/v0/status", this::status);
            server.createContext("/v0/entity", this::entity);
            server.start();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void stop() {
        if (server != null) {
            server.stop(0);
        }
    }

    private void status(HttpExchange ex) throws IOException {
        if (!"GET".equals(ex.getRequestMethod())) {
            ex.sendResponseHeaders(405, -1);
            return;
        }
        ex.sendResponseHeaders(200, -1);
    }

    private void entity(HttpExchange ex) throws IOException {

        String id = extractId(ex.getRequestURI());
        if (id == null || id.isBlank()) {
            ex.sendResponseHeaders(400, -1);
            return;
        }

        String owner = hasher.selectNode(id, nodes);

        if (!self.equals(owner)) {
            proxy.proxy(ex, owner);
            return;
        }

        switch (ex.getRequestMethod()) {
            case "GET" -> get(ex, id);
            case "PUT" -> put(ex, id);
            case "DELETE" -> delete(ex, id);
            default -> ex.sendResponseHeaders(405, -1);
        }
    }

    private void get(HttpExchange ex, String id) throws IOException {
        try {
            byte[] val = dao.get(id);
            ex.sendResponseHeaders(200, val.length);
            ex.getResponseBody().write(val);
        } catch (NoSuchElementException e) {
            ex.sendResponseHeaders(404, -1);
        }
    }

    private void put(HttpExchange ex, String id) throws IOException {
        byte[] body = ex.getRequestBody().readAllBytes();
        dao.upsert(id, body);
        ex.sendResponseHeaders(201, -1);
    }

    private void delete(HttpExchange ex, String id) throws IOException {
        dao.delete(id);
        ex.sendResponseHeaders(202, -1);
    }

    private String extractId(URI uri) {
        String q = uri.getQuery();
        if (q == null) {
            return null;
        }

        for (String p : q.split("&")) {
            String[] kv = p.split("=");
            if (kv.length == 2 && "id".equals(kv[0])) {
                return kv[1];
            }
        }
        return null;
    }
}
