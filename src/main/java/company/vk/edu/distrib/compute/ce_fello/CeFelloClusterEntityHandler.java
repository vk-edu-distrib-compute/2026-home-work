package company.vk.edu.distrib.compute.ce_fello;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

final class CeFelloClusterEntityHandler implements HttpHandler {
    private final String localEndpoint;
    private final CeFelloKeyDistributor distributor;
    private final Dao<byte[]> dao;
    private final CeFelloClusterProxyClient proxyClient;

    CeFelloClusterEntityHandler(
            String localEndpoint,
            CeFelloKeyDistributor distributor,
            Dao<byte[]> dao,
            CeFelloClusterProxyClient proxyClient
    ) {
        this.localEndpoint = Objects.requireNonNull(localEndpoint, "localEndpoint");
        this.distributor = Objects.requireNonNull(distributor, "distributor");
        this.dao = Objects.requireNonNull(dao, "dao");
        this.proxyClient = Objects.requireNonNull(proxyClient, "proxyClient");
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        if (!CeFelloClusterHttpHelper.ENTITY_PATH.equals(exchange.getRequestURI().getPath())) {
            CeFelloClusterHttpHelper.sendEmpty(exchange, 404);
            return;
        }

        String requestMethod = exchange.getRequestMethod();
        if (!CeFelloClusterHttpHelper.isSupportedEntityMethod(requestMethod)) {
            CeFelloClusterHttpHelper.sendEmpty(exchange, 405);
            return;
        }

        Map<String, String> queryParameters = CeFelloClusterHttpHelper.parseQuery(
                exchange.getRequestURI().getRawQuery()
        );
        String id = queryParameters.get(CeFelloClusterHttpHelper.ID_PARAMETER);
        if (id == null || id.isEmpty()) {
            CeFelloClusterHttpHelper.sendEmpty(exchange, 400);
            return;
        }

        String ownerEndpoint = distributor.ownerFor(id);
        if (localEndpoint.equals(ownerEndpoint) || CeFelloClusterHttpHelper.isProxied(exchange)) {
            handleLocally(exchange, id, requestMethod);
            return;
        }

        proxyClient.forward(exchange, ownerEndpoint, requestMethod);
    }

    private void handleLocally(HttpExchange exchange, String id, String requestMethod) throws IOException {
        switch (requestMethod) {
            case CeFelloClusterHttpHelper.GET_METHOD -> handleGet(exchange, id);
            case CeFelloClusterHttpHelper.PUT_METHOD -> handlePut(exchange, id);
            case CeFelloClusterHttpHelper.DELETE_METHOD -> handleDelete(exchange, id);
            default -> throw new IllegalArgumentException("Unsupported method: " + requestMethod);
        }
    }

    private void handleGet(HttpExchange exchange, String id) throws IOException {
        byte[] value = dao.get(id);
        CeFelloClusterHttpHelper.sendBody(exchange, 200, value);
    }

    private void handlePut(HttpExchange exchange, String id) throws IOException {
        dao.upsert(id, exchange.getRequestBody().readAllBytes());
        CeFelloClusterHttpHelper.sendEmpty(exchange, 201);
    }

    private void handleDelete(HttpExchange exchange, String id) throws IOException {
        dao.delete(id);
        CeFelloClusterHttpHelper.sendEmpty(exchange, 202);
    }
}
