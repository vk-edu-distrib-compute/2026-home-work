package company.vk.edu.distrib.compute.nihuaway00;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.nihuaway00.app.KVCommandService;
import company.vk.edu.distrib.compute.nihuaway00.transport.http.EntityHttpHandler;
import company.vk.edu.distrib.compute.nihuaway00.transport.grpc.InternalGrpcService;
import company.vk.edu.distrib.compute.nihuaway00.transport.http.StatusHttpHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class NodeServer implements company.vk.edu.distrib.compute.ReplicatedService {
    private static final Logger log = LoggerFactory.getLogger(NodeServer.class);

    private final KVCommandService commandService;
    private HttpServer server;
    private InternalGrpcService grpcServer;
    int port;

    public NodeServer(int port, KVCommandService commandService) {
        this.port = port;
        this.commandService = commandService;
        this.grpcServer = new InternalGrpcService(commandService);
    }

    @Override
    public void start() {
        try {
            InetSocketAddress addr = new InetSocketAddress(port);
            server = HttpServer.create(addr, 0);
            server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            registerContexts();
            server.start();

            grpcServer.newGrpcServer(port + 1);
            grpcServer.start();
        } catch (Exception e) {
            if (log.isErrorEnabled()) {
                log.error(e.getMessage());
            }
        }
    }

    @Override
    public void stop() {
        if (grpcServer != null) {
            grpcServer.shutdown();
            grpcServer = null;
        }

        if (server != null) {
            server.stop(0);
            server = null;
        } else {
            if (log.isWarnEnabled()) {
                log.warn("Server is not started");
            }
        }
    }

    private void registerContexts() {
        server.createContext("/v0/entity", new EntityHttpHandler(commandService));
        server.createContext("/v0/status", new StatusHttpHandler());
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
