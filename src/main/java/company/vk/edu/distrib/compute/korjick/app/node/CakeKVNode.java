package company.vk.edu.distrib.compute.korjick.app.node;

import company.vk.edu.distrib.compute.korjick.adapters.input.grpc.CakeGrpcServer;
import company.vk.edu.distrib.compute.korjick.adapters.input.http.CakeHttpServer;
import company.vk.edu.distrib.compute.korjick.adapters.input.http.entity.EntityHandler;
import company.vk.edu.distrib.compute.korjick.adapters.input.http.status.StatusHandler;
import company.vk.edu.distrib.compute.korjick.core.application.coordinator.KVCoordinator;
import company.vk.edu.distrib.compute.korjick.core.application.coordinator.SingleNodeKVCoordinator;
import company.vk.edu.distrib.compute.korjick.ports.output.EntityRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

public class CakeKVNode implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(CakeKVNode.class);

    private final EntityRepository repository;
    private final String host;
    private final int port;
    private final KVCoordinator httpCoordinator;
    private final String httpEndpoint;
    private final String grpcEndpoint;
    private CakeHttpServer httpServer;
    private CakeGrpcServer grpcServer;

    public CakeKVNode(EntityRepository repository,
                      String host,
                      int port) {
        this(repository, host, port, new SingleNodeKVCoordinator(repository));
    }

    public CakeKVNode(EntityRepository repository,
                      String host,
                      int port,
                      KVCoordinator coordinator) {
        this.repository = repository;
        this.host = host;
        this.port = port;
        this.httpCoordinator = coordinator;
        this.httpEndpoint = CakeHttpServer.resolveEndpoint(host, port);
        this.grpcEndpoint = CakeGrpcServer.resolveEndpoint(host, port);
    }

    public String httpEndpoint() {
        return httpEndpoint;
    }

    public String grpcEndpoint() {
        return grpcEndpoint;
    }

    public boolean isStarted() {
        return grpcServer != null;
    }

    public void start() {
        startGrpc();
        try {
            startHttp();
        } catch (RuntimeException e) {
            stopGrpc();
            throw e;
        }
    }

    public void stop() {
        stopHttp();
        stopGrpc();
    }

    public void startGrpc() {
        if (grpcServer != null) {
            return;
        }
        grpcServer = new CakeGrpcServer(host, port, repository);
        grpcServer.start();
        log.info("CakeKVNode gRPC started at {}", grpcEndpoint);
    }

    public void stopGrpc() {
        if (grpcServer == null) {
            return;
        }
        grpcServer.stop();
        grpcServer = null;
        log.info("CakeKVNode gRPC stopped at {}", grpcEndpoint);
    }

    @Override
    public void close() throws IOException {
        stop();
        repository.close();
    }

    private void startHttp() {
        if (httpServer != null) {
            return;
        }

        try {
            httpServer = new CakeHttpServer(host, port);
            httpServer.register("/v0/status", new StatusHandler());
            httpServer.register("/v0/entity", new EntityHandler(httpCoordinator));
            httpServer.start();
            log.info("CakeKVNode HTTP started at {}", httpServer.getEndpoint());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create HTTP server for node " + httpEndpoint, e);
        }
    }

    private void stopHttp() {
        if (httpServer == null) {
            return;
        }
        httpServer.stop();
        httpServer = null;
        log.info("CakeKVNode HTTP stopped at {}", httpEndpoint);
    }
}
