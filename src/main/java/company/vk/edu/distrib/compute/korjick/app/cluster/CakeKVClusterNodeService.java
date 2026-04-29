package company.vk.edu.distrib.compute.korjick.app.cluster;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.korjick.adapters.input.grpc.CakeGrpcServer;
import company.vk.edu.distrib.compute.korjick.adapters.input.http.CakeHttpServer;
import company.vk.edu.distrib.compute.korjick.adapters.input.http.entity.EntityHandler;
import company.vk.edu.distrib.compute.korjick.adapters.input.http.status.StatusHandler;
import company.vk.edu.distrib.compute.korjick.core.application.SingleNodeCoordinator;
import company.vk.edu.distrib.compute.korjick.ports.output.EntityRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

public class CakeKVClusterNodeService implements KVService, Closeable {
    private static final Logger log = LoggerFactory.getLogger(CakeKVClusterNodeService.class);

    private final EntityRepository entityRepository;
    private final String host;
    private final int port;
    private final SingleNodeCoordinator coordinator;
    private CakeHttpServer httpServer;
    private CakeGrpcServer grpcServer;

    public CakeKVClusterNodeService(EntityRepository entityRepository,
                                    String host,
                                    int port,
                                    SingleNodeCoordinator coordinator) {
        this.entityRepository = Objects.requireNonNull(entityRepository, "entityRepository");
        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator");
    }

    @Override
    public void start() {
        try {
            httpServer = new CakeHttpServer(host, port);
            httpServer.register("/v0/status", new StatusHandler());
            httpServer.register("/v0/entity", new EntityHandler(coordinator));
            grpcServer = new CakeGrpcServer(host, port, coordinator);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create cluster node servers", e);
        }
        grpcServer.start();
        httpServer.start();
        log.info("CakeKVClusterNodeService started at {}", httpServer.getEndpoint());
    }

    @Override
    public void stop() {
        if (httpServer != null) {
            httpServer.stop();
            httpServer = null;
        }
        if (grpcServer != null) {
            grpcServer.stop();
            grpcServer = null;
        }
        log.info("CakeKVClusterNodeService stopped");
    }

    @Override
    public void close() {
        stop();
        try {
            entityRepository.close();
        } catch (IOException e) {
            log.error("Failed to close EntityRepository", e);
        }
    }
}
