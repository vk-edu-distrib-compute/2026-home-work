package company.vk.edu.distrib.compute.korjick.app.cluster;

import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.korjick.adapters.input.grpc.CakeGrpcServer;
import company.vk.edu.distrib.compute.korjick.adapters.input.http.CakeHttpServer;
import company.vk.edu.distrib.compute.korjick.ports.output.EntityRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class CakeKVClusterNodeService implements KVService {
    private static final Logger log = LoggerFactory.getLogger(CakeKVClusterNodeService.class);

    private final EntityRepository entityRepository;
    private final CakeHttpServer httpServer;
    private final CakeGrpcServer grpcServer;

    public CakeKVClusterNodeService(EntityRepository entityRepository,
                                    CakeHttpServer httpServer,
                                    CakeGrpcServer grpcServer) {
        this.entityRepository = entityRepository;
        this.httpServer = httpServer;
        this.grpcServer = grpcServer;
    }

    @Override
    public void start() {
        grpcServer.start();
        httpServer.start();
        log.info("CakeKVClusterNodeService started at {}", httpServer.getEndpoint());
    }

    @Override
    public void stop() {
        httpServer.stop();
        grpcServer.stop();
        try {
            entityRepository.close();
        } catch (IOException e) {
            log.error("Failed to close EntityRepository", e);
        }
        log.info("CakeKVClusterNodeService stopped");
    }
}
