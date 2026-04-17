package company.vk.edu.distrib.compute.linempy.routing;

import java.util.List;

/**
 * Стратегия определения ноды для ключа.
 *
 * @author Linempy
 * @since 17.04.2026
 */
@FunctionalInterface
public interface ShardingStrategy {

    /**
     * Возвращает endpoint ноды, которая должна обработать ключ.
     *
     * @param key   ключ
     * @param nodes список всех нод кластера
     * @return endpoint целевой ноды
     */
    String route(String key, List<String> nodes);
}
