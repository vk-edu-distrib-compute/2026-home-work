package company.vk.edu.distrib.compute.korjick.core.application.node;

import company.vk.edu.distrib.compute.korjick.core.domain.Entity;
import company.vk.edu.distrib.compute.korjick.ports.output.EntityGateway;

import java.util.function.BooleanSupplier;

public class RemoteEntityNode implements EntityNode {
    private final String endpoint;
    private final EntityGateway gateway;

    public RemoteEntityNode(String endpoint,
                            EntityGateway gateway) {
        this.endpoint = endpoint;
        this.gateway = gateway;
    }

    @Override
    public String endpoint() {
        return endpoint;
    }

    @Override
    public Entity get(Entity.Key key) {
        return gateway.getEntity(endpoint, key);
    }

    @Override
    public void upsert(Entity entity) {
        gateway.upsertEntity(endpoint, entity);
    }

    @Override
    public void delete(Entity.Key key) {
        gateway.deleteEntity(endpoint, key);
    }
}
