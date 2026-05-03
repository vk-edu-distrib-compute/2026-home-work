package company.vk.edu.distrib.compute.shuuuurik.routing;

import java.util.List;

@FunctionalInterface
public interface NodeRouter {

    /**
     * Возвращает endpoint ноды, ответственной за данный ключ.
     *
     * @param key   ключ запроса (не пустой)
     * @param nodes список доступных endpoint'ов, например ["http://localhost:8080"]
     * @return endpoint выбранной ноды
     * @throws IllegalArgumentException если nodes пустой или null
     */
    String route(String key, List<String> nodes);
}
