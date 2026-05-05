package company.vk.edu.distrib.compute.korjick.core.application.node;

import company.vk.edu.distrib.compute.korjick.core.domain.Entity;

public interface EntityNode {
    String endpoint();

    Entity get(Entity.Key key);

    void upsert(Entity entity);

    void delete(Entity.Key key);
}
