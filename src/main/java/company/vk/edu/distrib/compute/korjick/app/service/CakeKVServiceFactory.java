package company.vk.edu.distrib.compute.korjick.app.service;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import company.vk.edu.distrib.compute.korjick.adapters.input.http.CakeHttpServer;
import company.vk.edu.distrib.compute.korjick.adapters.input.http.entity.EntityHandler;
import company.vk.edu.distrib.compute.korjick.adapters.input.http.status.StatusHandler;
import company.vk.edu.distrib.compute.korjick.adapters.output.H2EntityRepository;
import company.vk.edu.distrib.compute.korjick.core.application.coordinator.SingleNodeKVCoordinator;

import java.io.IOException;

public class CakeKVServiceFactory extends KVServiceFactory {
    @Override
    protected KVService doCreate(int port) throws IOException {
        final var repository = new H2EntityRepository(String.format("node_%s", port));
        final var httpServer = new CakeHttpServer("localhost", port);
        httpServer.register("/v0/status", new StatusHandler());
        httpServer.register("/v0/entity", new EntityHandler(new SingleNodeKVCoordinator(repository)));
        return new CakeKVService(
                repository,
                httpServer
        );
    }
}
