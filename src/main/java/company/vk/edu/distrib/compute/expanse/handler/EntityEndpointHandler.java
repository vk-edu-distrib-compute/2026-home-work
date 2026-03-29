package company.vk.edu.distrib.compute.expanse.handler;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.expanse.context.AppContextUtils;
import company.vk.edu.distrib.compute.expanse.model.ApiSettings;
import company.vk.edu.distrib.compute.expanse.model.HttpMethod;
import company.vk.edu.distrib.compute.expanse.service.HttpService;
import company.vk.edu.distrib.compute.expanse.service.impl.HttpServiceImpl;
import company.vk.edu.distrib.compute.expanse.utils.HttpUtils;
import company.vk.edu.distrib.compute.expanse.validator.HttpRequestValidatorUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class EntityEndpointHandler implements HttpHandler {
    private static final Logger log = Logger.getLogger(EntityEndpointHandler.class.getName());
    private static final String ID = "id";

    private final HttpService<String, byte[]> httpService;

    public EntityEndpointHandler() {
        this.httpService = AppContextUtils.getBean(HttpServiceImpl.class);
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

        // Взаимоисключающие проверки codestyle вынудили отказаться от switch с enum'ом.
        // Одна проверка требовала вставить default (потому что так принято), другая требовала его убрать
        // (так как учитывались все варианты перечисления).
        switch (method.name()) { 
            case "PUT" -> {
                String key = params.get(ID);
                byte[] body = exchange.getRequestBody().readAllBytes();
                httpService.putEntity(key, body);
                writeResponse(exchange, new byte[0], 201);
            }

            case "GET" -> {
                byte[] bytes = httpService.getEntityByID(params.get(ID));
                writeResponse(exchange, bytes, 200);
            }

            case "DELETE" -> {
                httpService.deleteEntityByID(params.get(ID));
                writeResponse(exchange, new byte[0], 202);
            }

            default -> throw new UnsupportedOperationException();
            
        }
        log.info("Finished processing request.");
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
}
