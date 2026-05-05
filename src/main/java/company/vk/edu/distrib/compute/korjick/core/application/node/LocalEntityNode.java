package company.vk.edu.distrib.compute.korjick.core.application.node;

import company.vk.edu.distrib.compute.korjick.core.application.exception.EntityNotFoundException;
import company.vk.edu.distrib.compute.korjick.core.application.exception.StorageFailureException;
import company.vk.edu.distrib.compute.korjick.core.domain.Entity;
import company.vk.edu.distrib.compute.korjick.ports.output.EntityRepository;

import java.io.IOException;
import java.util.NoSuchElementException;

public class LocalEntityNode implements EntityNode {
    private final String endpoint;
    private final EntityRepository repository;

    public LocalEntityNode(String endpoint,
                           EntityRepository repository) {
        this.endpoint = endpoint;
        this.repository = repository;
    }

    @Override
    public String endpoint() {
        return endpoint;
    }

    @Override
    public Entity get(Entity.Key key) {
        try {
            return repository.get(key.value());
        } catch (NoSuchElementException e) {
            throw new EntityNotFoundException("No value for key: " + key.value(), e);
        } catch (IOException e) {
            throw new StorageFailureException("Failed to get key: " + key.value(), e);
        }
    }

    @Override
    public void upsert(Entity entity) {
        try {
            repository.upsert(entity.key().value(), entity);
        } catch (IOException e) {
            throw new StorageFailureException("Failed to upsert key: " + entity.key().value(), e);
        }
    }

    @Override
    public void delete(Entity.Key key) {
        try {
            repository.delete(key.value());
        } catch (IOException e) {
            throw new StorageFailureException("Failed to delete key: " + key.value(), e);
        }
    }
}
