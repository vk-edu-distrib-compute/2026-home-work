package company.vk.edu.distrib.compute.korjick.http.entity;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import company.vk.edu.distrib.compute.korjick.http.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

public class EntityHandler implements HttpHandler {

    private static final Logger log = LoggerFactory.getLogger(EntityHandler.class);
    private static final String ID_QUERY_PARAM_VALUE = "id";

    private final EntityRequestProcessor requestProcessor;

    public EntityHandler(EntityRequestProcessor requestProcessor) {
        this.requestProcessor = requestProcessor;
    }

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
        final var method = httpExchange.getRequestMethod();
        final var id = parseIdQueryParam(httpExchange);
        if (id == null) {
            throw new IllegalArgumentException("query param id is required");
        }

        log.info("Received {} request for entity {}", method, id);
        byte[] body = Constants.HTTP_METHOD_PUT.equals(method) ? httpExchange.getRequestBody().readAllBytes() : null;
        boolean internal = Constants.INTERNAL_REQUEST_HEADER_VALUE.equals(
                httpExchange.getRequestHeaders().getFirst(Constants.INTERNAL_REQUEST_HEADER)
        );
        EntityResponse response = requestProcessor.process(new EntityRequest(method, id, body, internal));
        writeResponse(httpExchange, response);
    }

    private String parseIdQueryParam(HttpExchange httpExchange) {
        final var rawQuery = httpExchange.getRequestURI().getRawQuery();
        if (rawQuery == null || rawQuery.isEmpty()) {
            return null;
        }

        final var params = rawQuery.split(Constants.QUERY_PARAM_SEPARATOR);
        for (var param : params) {
            final var splitParamTuple = param.split(Constants.QUERY_VALUE_SEPARATOR, 2);
            if (splitParamTuple.length == 2 && ID_QUERY_PARAM_VALUE.equals(decodeQueryComponent(splitParamTuple[0]))) {
                return decodeQueryComponent(splitParamTuple[1]);
            } else if (splitParamTuple.length == 1
                    && ID_QUERY_PARAM_VALUE.equals(decodeQueryComponent(splitParamTuple[0]))) {
                return Constants.EMPTY_QUERY_PARAM_VALUE;
            }
        }

        return null;
    }

    private String decodeQueryComponent(String queryComponent) {
        return URLDecoder.decode(queryComponent, StandardCharsets.UTF_8);
    }

    private void writeResponse(HttpExchange exchange, EntityResponse response) throws IOException {
        byte[] body = response.body();
        if (body == null || body.length == 0) {
            exchange.sendResponseHeaders(response.statusCode(), Constants.EMPTY_BODY_LENGTH);
            return;
        }

        exchange.sendResponseHeaders(response.statusCode(), body.length);
        exchange.getResponseBody().write(body);
    }
}
