package company.vk.edu.distrib.compute.korjick.core.application.coordinator;

import company.vk.edu.distrib.compute.korjick.core.domain.Entity;

public interface KVCoordinator {
    Entity get(Entity.Key key);

    void upsert(Entity entity);

    void delete(Entity.Key key);
}
