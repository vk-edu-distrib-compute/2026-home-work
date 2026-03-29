package company.vk.edu.distrib.compute.expanse.handler;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.expanse.exception.HttpRequestException;
import company.vk.edu.distrib.compute.expanse.model.ApiSettings;
import company.vk.edu.distrib.compute.expanse.validator.HttpRequestValidatorUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Logger;

public class HandlerWrapper implements HttpHandler {
    private static final Logger log = Logger.getLogger(HandlerWrapper.class.getName());

    private final ApiSettings apiSettings;
    private final HttpHandler handler;

    public HandlerWrapper(ApiSettings apiSettings, HttpHandler handler) {
        this.apiSettings = apiSettings;
        this.handler = handler;
    }

    @Override
    public void handle(HttpExchange exchange) throws IOException {
        // codestyle чекер крайне настаивал на закрытии exchange не в блоке finally,
        // что вынудило создать лишнюю вложенность, иначе он преждевременно закрывался.
        try (exchange) {
            try {
                HttpRequestValidatorUtils.checkIsSupportedMethod(apiSettings, exchange.getRequestMethod());
                handler.handle(exchange);

            } catch (HttpRequestException e) {
                handleErrorResponse(exchange, e.getCode(), e.getMessage());

            } catch (Exception e) {
                handleErrorResponse(exchange, 500, e.getMessage());
            }
        }

    }

    private void handleErrorResponse(HttpExchange exchange, int code, String message) throws IOException {
        log.info(message);
        byte[] body = message.getBytes();

        if (body.length == 0) {
            exchange.sendResponseHeaders(code, -1);
            return;
        }
        exchange.sendResponseHeaders(code, body.length);

        OutputStream os = exchange.getResponseBody();
        os.write(body);
        os.close();
    }
}
