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

    String route(String key, List<String> nodes);
}
