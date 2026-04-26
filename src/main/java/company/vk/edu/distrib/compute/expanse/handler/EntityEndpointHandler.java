package company.vk.edu.distrib.compute.expanse.handler;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.expanse.algorithm.RendezvousHashing;
import company.vk.edu.distrib.compute.expanse.client.GrpcClient;
import company.vk.edu.distrib.compute.expanse.core.KVShardingClusterImpl;
import company.vk.edu.distrib.compute.expanse.context.AppContextUtils;
import company.vk.edu.distrib.compute.expanse.dto.proto.GetEntityResponse;
import company.vk.edu.distrib.compute.expanse.exception.EntityNotFoundException;
import company.vk.edu.distrib.compute.expanse.model.ApiSettings;
import company.vk.edu.distrib.compute.expanse.model.HttpMethod;
import company.vk.edu.distrib.compute.expanse.service.KVStorageService;
import company.vk.edu.distrib.compute.expanse.service.impl.KVStorageServiceImpl;
import company.vk.edu.distrib.compute.expanse.utils.HttpUtils;
import company.vk.edu.distrib.compute.expanse.validator.HttpRequestValidatorUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

public class EntityEndpointHandler implements HttpHandler {
    private static final Logger log = Logger.getLogger(EntityEndpointHandler.class.getName());
    private static final String ID = "id";
    private static final int NOT_FOUND = 404;

    private final KVStorageService<String, byte[]> kvStorageService;

    public EntityEndpointHandler() {
        this.kvStorageService = AppContextUtils.getBean(KVStorageServiceImpl.class);
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        HttpMethod method = HttpMethod.valueOf(exchange.getRequestMethod());
        if (log.isLoggable(Level.INFO)) {
            log.info(String.format("Starting %s request for path %s.",
                    method.name(), exchange.getRequestURI().toString())
            );
        }
        Map<String, String> params = HttpUtils.extractParams(exchange);
        HttpRequestValidatorUtils.checkContainsAllRequiredParams(ApiSettings.ENTITY_ENDPOINT, params);

        String key = params.get(ID);

        if (KVShardingClusterImpl.isShardingEnabled()) {
            UUID targetUid = RendezvousHashing.getCorrespondingUid(key, KVShardingClusterImpl.getShardKeys());

            int port = exchange.getLocalAddress().getPort();
            UUID currentUid = KVShardingClusterImpl.getPortShardKeyMap().get(port);

            if (!currentUid.equals(targetUid)) {
                redirect(exchange, key, method.name(), targetUid);
                log.info("Finished redirecting HTTP request via gRPC.");
                return;
            }
        }

        switch (method.name()) { 
            case "PUT" -> {
                byte[] body = exchange.getRequestBody().readAllBytes();
                kvStorageService.putEntity(key, body);
                writeResponse(exchange, new byte[0], 201);
            }

            case "GET" -> {
                byte[] bytes = kvStorageService.getEntityByID(key);
                writeResponse(exchange, bytes, 200);
            }

            case "DELETE" -> {
                kvStorageService.deleteEntityByID(key);
                writeResponse(exchange, new byte[0], 202);
            }

            default -> throw new UnsupportedOperationException();
            
        }
        log.info("Finished processing HTTP request.");
    }

    private void writeResponse(HttpExchange exchange, byte[] bytes, int code) throws IOException {
        if (bytes.length == 0) {
            exchange.sendResponseHeaders(code, -1);
            return;
        }
        exchange.sendResponseHeaders(code, bytes.length);

        OutputStream os = exchange.getResponseBody();
        os.write(bytes);
        os.flush();
    }

    private void redirect(HttpExchange exchange, String key, String method, UUID targetUid) throws IOException {
        String endpoint = KVShardingClusterImpl.getShardKeyEndpointMap().get(targetUid);
        int grpcPort = KVShardingClusterImpl.getEndpointNodeMap().get(endpoint).getGrpcPort();

        switch (method) {
            case "PUT" -> {
                byte[] body = exchange.getRequestBody().readAllBytes();
                GrpcClient.putEntity(key, body, grpcPort);
                writeResponse(exchange, new byte[0], 201);
            }

            case "GET" -> {
                GetEntityResponse response = GrpcClient.getEntity(key, grpcPort);
                if (response.getCode() == NOT_FOUND) {
                    throw new EntityNotFoundException("Entity with id " + key + " not found.", null);
                }
                writeResponse(exchange, response.getResponse().toByteArray(), 200);
            }

            case "DELETE" -> {
                GrpcClient.deleteEntity(key, grpcPort);
                writeResponse(exchange, new byte[0], 202);
            }

            default -> throw new UnsupportedOperationException();
        }
    }
}
