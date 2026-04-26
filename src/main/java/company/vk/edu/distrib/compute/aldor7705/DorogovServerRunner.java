package company.vk.edu.distrib.compute.aldor7705;

import company.vk.edu.distrib.compute.KVService;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public class DorogovServerRunner {
    private int getReplicasFromArgs(String... args) {
        for (String arg : args) {
            if (arg.startsWith("--replicas=")) {
                return Integer.parseInt(arg.substring("--replicas=".length()));
            }
        }

        String env = System.getenv("REPLICATION_FACTOR");
        if (env != null && !env.isEmpty()) {
            return Integer.parseInt(env);
        }

        try (InputStream in = Files.newInputStream(Path.of("config.properties"))) {
            Properties properties = new Properties();
            properties.load(in);
            String value = properties.getProperty("replica");
            if (value != null && !value.isEmpty()) {
                return Integer.parseInt(properties.getProperty("replica"));
            }
        } catch (Exception e) {
            // дефолт
        }

        return 1;
    }

    void main(String... args) throws IOException {
        var log = LoggerFactory.getLogger("server");
        int replicas = getReplicasFromArgs();
        var port = 8080;
        KVService storage = new KVServiceFactorySimple("storage", null, replicas).create(port);
        storage.start();
        log.info("Server started on port {}", port);
        Runtime.getRuntime().addShutdownHook(new Thread(storage::stop));
    }
}
