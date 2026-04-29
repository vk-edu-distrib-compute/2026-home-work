package company.vk.edu.distrib.compute.korjick.core.application;

import company.vk.edu.distrib.compute.korjick.core.domain.Entity;
import company.vk.edu.distrib.compute.korjick.ports.output.EntityGateway;
import company.vk.edu.distrib.compute.korjick.ports.output.EntityRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

public class ShardingKVCoordinator extends SingleNodeCoordinator {
    private static final Logger log = LoggerFactory.getLogger(ShardingKVCoordinator.class);

    private final String currentEndpoint;
    private final List<String> endpoints;
    private final EntityGateway gateway;

    public ShardingKVCoordinator(EntityRepository entityRepository,
                                 String currentEndpoint,
                                 List<String> endpoints,
                                 EntityGateway gateway) {
        super(entityRepository);
        this.currentEndpoint = Objects.requireNonNull(currentEndpoint, "currentEndpoint");
        this.endpoints = List.copyOf(endpoints);
        this.gateway = gateway;
    }

    @Override
    public Entity get(Entity.Key key) {
        String target = resolve(key.value());
        if (isCurrentNode(target)) {
            return super.get(key);
        }

        log.info("Proxying get request to: {} for key: {}", target, key.value());
        return gateway.getEntity(target, key);
    }

    @Override
    public void upsert(Entity entity) {
        String target = resolve(entity.key().value());
        if (isCurrentNode(target)) {
            super.upsert(entity);
            return;
        }

        log.info("Proxying upsert request to: {} for key: {}", target, entity.key().value());
        gateway.upsertEntity(target, entity);
    }

    @Override
    public void delete(Entity.Key key) {
        String target = resolve(key.value());
        if (isCurrentNode(target)) {
            super.delete(key);
            return;
        }

        log.info("Proxying delete request to: {} for key: {}", target, key.value());
        gateway.deleteEntity(target, key);
    }

    private String resolve(String key) {
        String selected = null;
        long bestScore = 0;
        for (String endpoint : endpoints) {
            long score = Integer.toUnsignedLong(Objects.hash(key, endpoint));
            if (selected == null || score > bestScore) {
                bestScore = score;
                selected = endpoint;
            }
        }
        if (selected == null) {
            throw new IllegalArgumentException("No endpoints registered");
        }
        return selected;
    }

    private boolean isCurrentNode(String endpoint) {
        return Objects.equals(currentEndpoint, endpoint);
    }
}
