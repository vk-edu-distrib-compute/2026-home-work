package company.vk.edu.distrib.compute.nihuaway00;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.nihuaway00.app.KVCommandService;
import company.vk.edu.distrib.compute.nihuaway00.transport.http.EntityHttpHandler;
import company.vk.edu.distrib.compute.nihuaway00.transport.grpc.InternalGrpcService;
import company.vk.edu.distrib.compute.nihuaway00.transport.http.StatusHttpHandler;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.protobuf.services.ProtoReflectionService;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class NodeServer implements company.vk.edu.distrib.compute.ReplicatedService {
    private final KVCommandService commandService;
    private HttpServer httpServer;
    private Server grpcServer;
    private final int port;
    private final int grpcPort;

    public NodeServer(int port, KVCommandService commandService) {
        this(port, port + 1, commandService);
    }

    public NodeServer(int port, int grpcPort, KVCommandService commandService) {
        this.port = port;
        this.grpcPort = grpcPort;
        this.commandService = commandService;
    }

    @Override
    public synchronized void start() {
        if (httpServer != null || grpcServer != null) {
            return;
        }

        try {
            grpcServer = Grpc.newServerBuilderForPort(grpcPort, InsecureServerCredentials.create())
                    .addService(new InternalGrpcService(commandService))
                    .addService(ProtoReflectionService.newInstance())
                    .build();
            grpcServer.start();

            httpServer = HttpServer.create(new InetSocketAddress(port), 0);
            httpServer.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            registerContexts();
            httpServer.start();
        } catch (Exception e) {
            stop();
            throw new IllegalStateException("Failed to start node on port " + port, e);
        }
    }

    @Override
    public synchronized void stop() {
        if (grpcServer != null) {
            Server serverToStop = grpcServer;
            grpcServer = null;
            serverToStop.shutdown();
            try {
                if (!serverToStop.awaitTermination(10, TimeUnit.SECONDS)) {
                    serverToStop.shutdownNow();
                    serverToStop.awaitTermination(10, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                serverToStop.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        if (httpServer != null) {
            httpServer.stop(0);
            httpServer = null;
        }
    }

    private void registerContexts() {
        httpServer.createContext("/v0/entity", new EntityHttpHandler(commandService));
        httpServer.createContext("/v0/status", new StatusHttpHandler());
    }

    @Override
    public int port() {
        return port;
    }

    @Override
    public int numberOfReplicas() {
        return commandService.replicaManager.numberOfReplicas();
    }

    @Override
    public void disableReplica(int nodeId) {
        commandService.replicaManager.disableReplica(nodeId);
    }

    @Override
    public void enableReplica(int nodeId) {
        commandService.replicaManager.enableReplica(nodeId);
    }
}
