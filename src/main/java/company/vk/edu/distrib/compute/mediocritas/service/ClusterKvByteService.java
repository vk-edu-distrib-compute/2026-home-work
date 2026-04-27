package company.vk.edu.distrib.compute.mediocritas.service;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.mediocritas.cluster.proxy.HttpProxyClient;
import company.vk.edu.distrib.compute.mediocritas.cluster.proxy.ProxyClient;
import company.vk.edu.distrib.compute.mediocritas.cluster.routing.Router;

import java.io.IOException;
import java.net.http.HttpResponse;

public class ClusterKvByteService extends AbstractKvByteService {

    protected final String localEndpoint;
    protected final Router router;
    protected final ProxyClient proxyClient;

    public ClusterKvByteService(
            int port,
            Dao<byte[]> dao,
            Router router,
            ProxyClient proxyClient
    ) {
        super(port, dao);
        this.localEndpoint = "http://localhost:" + port;
        this.router = router;
        this.proxyClient = proxyClient;
    }

    @Override
    protected void handleEntity(HttpExchange http) throws IOException {
        String id = parseId(http.getRequestURI().getQuery());

        String ownerEndpoint = router.getNodeForKey(id);
        if (!ownerEndpoint.equals(localEndpoint)) {
            sendProxyRequest(http, ownerEndpoint, id);
            return;
        }

        handleEntityLocally(http, id);
    }

    private void sendProxyRequest(HttpExchange http, String targetEndpoint, String key) throws IOException {
        try {
            HttpResponse<?> response;
            switch (http.getRequestMethod()) {
                case "GET" -> {
                    HttpResponse<byte[]> getResponse = proxyClient.proxyGet(targetEndpoint, key);
                    http.sendResponseHeaders(getResponse.statusCode(), getResponse.body().length);
                    sendBody(http, getResponse.body());
                }
                case "PUT" -> {
                    byte[] body = http.getRequestBody().readAllBytes();
                    response = proxyClient.proxyPut(targetEndpoint, key, body);
                    http.sendResponseHeaders(response.statusCode(), -1);
                }
                case "DELETE" -> {
                    response = proxyClient.proxyDelete(targetEndpoint, key);
                    http.sendResponseHeaders(response.statusCode(), -1);
                }
                default -> http.sendResponseHeaders(405, -1);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Proxy request interrupted", e);
        }
    }
}
