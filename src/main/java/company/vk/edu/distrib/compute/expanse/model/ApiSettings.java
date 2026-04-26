package company.vk.edu.distrib.compute.expanse.model;

import company.vk.edu.distrib.compute.expanse.handler.EntityEndpointHandler;
import company.vk.edu.distrib.compute.expanse.handler.StatusEndpointHandler;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static company.vk.edu.distrib.compute.expanse.model.HttpMethod.*;

/**
 * Перечисление, определяющее настройки доступных API-эндпоинтов приложения.
 */
public enum ApiSettings {
    STATUS_ENDPOINT(
            "/v0/status",
            List.of(GET),
            List.of(),
            StatusEndpointHandler.class
    ),
    ENTITY_ENDPOINT(
            "/v0/entity",
            List.of(GET, PUT, DELETE),
            List.of("id"),
            EntityEndpointHandler.class
    );

    private final String path;
    private final Set<String> supportedMethods;
    private final Set<String> requiredParams;
    private final Class<?> handlerClass;

    ApiSettings(
            String path,
            List<HttpMethod> supportedMethods,
            List<String> requiredParams,
            Class<?> handlerClass
    ) {
        this.path = path;
        this.supportedMethods = supportedMethods.stream()
                .map(Enum::name)
                .collect(Collectors.toUnmodifiableSet());
        this.requiredParams = requiredParams.stream()
                .map(String::toLowerCase)
                .collect(Collectors.toUnmodifiableSet());
        this.handlerClass = handlerClass;
    }

    public String getPath() {
        return path;
    }

    public Set<String> getSupportedMethods() {
        return supportedMethods;
    }

    public Set<String> getRequiredParams() {
        return requiredParams;
    }

    public Class<?> getHandlerClass() {
        return handlerClass;
    }
}
