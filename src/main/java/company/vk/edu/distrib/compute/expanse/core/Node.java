package company.vk.edu.distrib.compute.expanse.core;

import company.vk.edu.distrib.compute.expanse.handler.GrpcHandler;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

public class Node {
    private HttpKVService kvService;
    private Server grpcServer;
    private final String httpEndpoint;
    private final int httpPort;
    private final int grpcPort;
    private boolean started;
    private boolean stopped;

    public Node(int httpPort, int grpcPort) throws IOException {
        this.kvService = new HttpKVService(httpPort);
        this.httpEndpoint = "http://localhost:" + httpPort + "?" + "grpcPort=" + grpcPort;
        this.httpPort = httpPort;
        this.grpcPort = grpcPort;
        grpcServer = ServerBuilder.forPort(grpcPort)
                .addService(new GrpcHandler())
                .build();
    }

    public void start() throws IOException {
        if (stopped) {
            kvService = new HttpKVService(httpPort);
            grpcServer = ServerBuilder.forPort(grpcPort)
                    .addService(new GrpcHandler())
                    .build();
            started = false;
            stopped = false;
        }
        if (started) {
            return;
        }
        kvService.start();
        grpcServer.start();
        this.started = true;
    }

    public void stop() {
        if (stopped || !started) {
            return;
        }
        kvService.stop();
        grpcServer.shutdown();
        this.stopped = true;
    }

    public String getEndpoint() {
        return httpEndpoint;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public int getGrpcPort() {
        return grpcPort;
    }
}
