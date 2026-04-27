package company.vk.edu.distrib.compute.mediocritas.service;

import com.sun.net.httpserver.HttpExchange;
import company.vk.edu.distrib.compute.Dao;
import company.vk.edu.distrib.compute.mediocritas.cluster.proxy.ProxyClient;
import company.vk.edu.distrib.compute.mediocritas.cluster.routing.Router;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class GrpcKvByteService extends ClusterKvByteService {

    private final int grpcPort;

    public GrpcKvByteService(
            int port,
            int grpcPort,
            Dao<byte[]> dao,
            Router router,
            ProxyClient proxyClient
    ) {
        super(port, dao, router, proxyClient);
        this.grpcPort = grpcPort;
    }




    @Override
    protected void handleEntity(HttpExchange http) throws IOException {

    }

    @Override
    public CompletableFuture<Void> awaitTermination() {
        return super.awaitTermination();
    }
}
