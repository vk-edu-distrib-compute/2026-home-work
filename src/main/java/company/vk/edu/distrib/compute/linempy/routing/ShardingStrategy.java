package company.vk.edu.distrib.compute.linempy.routing;

import java.util.List;

/**
 * NodeRouter — описание интерфейса.
 *
 * <p>
 * TODO: описать, какие обязанности реализует интерфейс.
 * </p>
 *
 * @author Linempy
 * @since 17.04.2026
 */
@FunctionalInterface
public interface ShardingStrategy {

    String route(String key, List<String> nodes);
}
