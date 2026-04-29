package company.vk.edu.distrib.compute.korjick.ports.output;

import company.vk.edu.distrib.compute.korjick.core.domain.Entity;

public interface EntityGateway {
    Entity getEntity(String endpoint, Entity.Key key);

    void upsertEntity(String endpoint, Entity entity);

    void deleteEntity(String endpoint, Entity.Key key);
}
