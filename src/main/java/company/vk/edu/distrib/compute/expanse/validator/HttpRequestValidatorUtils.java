package company.vk.edu.distrib.compute.expanse.validator;

import company.vk.edu.distrib.compute.expanse.exception.HttpBadRequestException;
import company.vk.edu.distrib.compute.expanse.exception.HttpMethodNotSupportedException;
import company.vk.edu.distrib.compute.expanse.model.ApiSettings;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public final class HttpRequestValidatorUtils {
    private HttpRequestValidatorUtils() {
    }

    public static void checkIsSupportedMethod(ApiSettings config, String method) {
        if (!config.getSupportedMethods().contains(method)) {
            throw new HttpMethodNotSupportedException(
                    String.format("Http method %s is not supported", method)
            );
        }
    }

    public static void checkContainsAllRequiredParams(ApiSettings config, Map<String, String> paramsMap) {
        Set<String> nonNullParamsKeys = paramsMap.entrySet().stream()
                .filter(entry -> Objects.nonNull(entry.getValue()) && !entry.getValue().isBlank())
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

        if (!nonNullParamsKeys.containsAll(config.getRequiredParams())) {
            throw new HttpBadRequestException("Missing one or more required query parameters");
        }
    }
}
