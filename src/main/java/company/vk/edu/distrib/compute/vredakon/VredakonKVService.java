package company.vk.edu.distrib.compute.vredakon;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.KVService;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VredakonKVService implements KVService {
    private static final Logger log = LoggerFactory.getLogger(VredakonKVService.class);

    private HttpServer server;
    private final Dao<byte[]> dao;
    private final HttpClient client = HttpClient.newHttpClient();
    private final AtomicBoolean isStarted = new AtomicBoolean(false);
    private final int port;
    private Map<String, VredakonKVService> otherNodes = new ConcurrentHashMap<>();

    public VredakonKVService(int port) throws IOException {
        this.port = port;
        this.dao = new FileSystemDaoImpl(String.valueOf(port));
    }

    private void initServer() {
        if (server == null) {
            log.error("Server is null");
        } else {
            server.createContext("/v0/status", this::handleStatus);
            server.createContext("/v0/entity", this::handleEntity);
            log.info("Server initialized");
        }
    }

    public void setOtherNodes(Map<String, VredakonKVService> otherNodes) {
        this.otherNodes = otherNodes;
    }

    @Override
    public synchronized void start() {
        if (!isStarted.get()) {
            try {
                server = HttpServer.create(new InetSocketAddress(port), 0);
                initServer();
                server.start();
                isStarted.set(true);
            } catch (IOException exc) {
                log.error("some error");
            }
        }
    }

    @Override
    public synchronized void stop() {
        if (isStarted.get()) {
            server.stop(0);
            isStarted.set(false);
        }
    }

    public void handleEntity(HttpExchange exchange) throws IOException {

        String query = exchange.getRequestURI().getQuery();

        if (query == null) {
            exchange.sendResponseHeaders(400, -1);
            return;
        }

        Map<String, String> params = new ConcurrentHashMap<>();
        for (String param : query.split("&")) {
            String[] keyValue = param.split("=");
            try {
                params.put(keyValue[0], keyValue[1]);
            } catch (ArrayIndexOutOfBoundsException exc) {
                exchange.sendResponseHeaders(400, -1);
                return;
            }
        }

        String appropriateNode = "http://localhost:" + server.getAddress().getPort();
        if (!otherNodes.isEmpty()) {
            appropriateNode = Strategies.resolve(params.get("id"), otherNodes);
        }
        
        boolean doProxy = !appropriateNode.equals("http://localhost:" + server.getAddress().getPort()) && !otherNodes.isEmpty();

        switch (exchange.getRequestMethod()) {
            case "GET":
                handleEntityGet(exchange, doProxy, appropriateNode, params);
                break;

            case "PUT":
                handleEntityPut(exchange, doProxy, appropriateNode, params);
                break;

            case "DELETE":
                handleEntityDelete(exchange, doProxy, appropriateNode, params);
                break;
            default:
                break;
        }
        exchange.close();
    }

    public void handleStatus(HttpExchange exchange) throws IOException {
        exchange.sendResponseHeaders(200, -1);
        exchange.close();
    }

    private HttpResponse<byte[]> proxyRequest(String url, String method, byte[] data) {
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder();
        requestBuilder.uri(URI.create(url));
        if ("PUT".equals(method)) {
            requestBuilder.method(method, HttpRequest.BodyPublishers.ofByteArray(data));
        } else {
            requestBuilder.method(method, HttpRequest.BodyPublishers.noBody());
        }
        HttpResponse<byte[]> response = null;
        try {
            response = client.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofByteArray());
        } catch (InterruptedException | IOException exc) {
            log.error("Some error");
        }
        return response;
    }

    private void handleEntityGet(HttpExchange exchange, 
    boolean doProxy, 
    String appropriateNode,
    Map<String, String> params) throws IOException {
        try (OutputStream os = exchange.getResponseBody()) {
            if (doProxy) {
                HttpResponse<byte[]> response = proxyRequest(
                        appropriateNode + "/v0/entity?id=" + params.get("id"),
                        "GET", null);
                exchange.sendResponseHeaders(response.statusCode(), response.body().length);
                os.write(response.body());
                exchange.close();
                return;
            }
            byte[] data = dao.get(params.get("id"));
            exchange.sendResponseHeaders(200, data.length);
            os.write(data);
        } catch (Exception e) {
            exchange.sendResponseHeaders(404, -1);
        }
    }

    private void handleEntityPut(HttpExchange exchange, 
    boolean doProxy, 
    String appropriateNode,
    Map<String, String> params) throws IOException {
        byte[] data = exchange.getRequestBody().readAllBytes();
        if (doProxy) {
            exchange.sendResponseHeaders(proxyRequest(appropriateNode + "/v0/entity?id=" + params.get("id"),
            "PUT", data).statusCode(), -1);
            exchange.close();
            return;
        }
        dao.upsert(params.get("id"), data);
        exchange.sendResponseHeaders(201, -1);
    }

    private void handleEntityDelete(HttpExchange exchange, 
    boolean doProxy, 
    String appropriateNode,
    Map<String, String> params) throws IOException {
        if (doProxy) {
            exchange.sendResponseHeaders(proxyRequest(appropriateNode + "/v0/entity?id=" + params.get("id"),
            "DELETE", null).statusCode(), -1);
            exchange.close();
            return;
        }
        dao.delete(params.get("id"));
        exchange.sendResponseHeaders(202, -1);
    }
}
