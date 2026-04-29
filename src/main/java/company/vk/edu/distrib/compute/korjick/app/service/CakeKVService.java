package company.vk.edu.distrib.compute.korjick.app.service;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.korjick.adapters.input.http.CakeHttpServer;
import company.vk.edu.distrib.compute.korjick.ports.output.EntityRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CakeKVService implements KVService {

    private static final Logger log = LoggerFactory.getLogger(CakeKVService.class);

    private final EntityRepository entityRepository;
    private final CakeHttpServer httpServer;

    public CakeKVService(EntityRepository entityRepository,
                         CakeHttpServer httpServer) {
        this.entityRepository = entityRepository;
        this.httpServer = httpServer;
    }

    @Override
    public void start() {
        httpServer.start();
        log.info("CakeKVService started at {}", httpServer.getEndpoint());
    }

    @Override
    public void stop() {
        try {
            entityRepository.close();
        } catch (IOException e) {
            log.error("Failed to close EntityRepository", e);
        }
        httpServer.stop();
        log.info("CakeKVService stopped");
    }
}

