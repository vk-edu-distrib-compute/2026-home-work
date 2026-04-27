package company.vk.edu.distrib.compute.golubtsov_pavel;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class PGgrpcKVService extends PGInMemoryKVService {
    public PGgrpcKVService(int port) throws IOException {
        this(port, PGPorts.availablePort(List.of(port)));
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
}
