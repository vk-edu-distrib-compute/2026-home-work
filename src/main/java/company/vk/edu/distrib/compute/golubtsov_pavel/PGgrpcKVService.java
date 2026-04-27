package company.vk.edu.distrib.compute.golubtsov_pavel;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

public class PGgrpcKVService extends PGInMemoryKVService {
    public PGgrpcKVService(int port) throws IOException {
        this(port, Ports.availablePort(List.of(port)));
    }

    private PGgrpcKVService(int port, int grpcPort) throws IOException {
        super(
                port,
                grpcPort,
                new PGFileDao(Path.of("PGData", String.valueOf(port))),
                "http://localhost:" + port + "?grpcPort=" + grpcPort,
                List.of("http://localhost:" + port + "?grpcPort=" + grpcPort)
        );
    }

    static final class Ports {
        private static final int BACKLOG = 1;

        private Ports() {
        }

        static int availablePort(Collection<Integer> excludedPorts) throws IOException {
            try (ServerSocket socket = new ServerSocket()) {
                socket.setReuseAddress(false);
                socket.bind(new InetSocketAddress(InetAddress.getByName("localhost"), 0), BACKLOG);
                int port = socket.getLocalPort();
                if (excludedPorts.contains(port)) {
                    return availablePort(excludedPorts);
                }
                return port;
            }
        }
    }
}
