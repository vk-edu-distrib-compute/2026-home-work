package company.vk.edu.distrib.compute.aldor7705;

import company.vk.edu.distrib.compute.KVService;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class DorogovServer {
    private static int getReplicasFromArgs(String... args) {
        for (String arg : args) {
            if (arg.startsWith("--replicas=")) {
                return Integer.parseInt(arg.substring("--replicas=".length()));
            }
        }

        String env = System.getenv("REPLICATION_FACTOR");
        if (env != null && !env.isEmpty()) {
            return Integer.parseInt(env);
        }

        try (FileInputStream in = new FileInputStream("src/main/java/company/vk/edu/distrib/compute/aldor7705/config.properties")) {
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

    static void main(String... args) throws IOException {
        var log = LoggerFactory.getLogger("server");
        int replicas = 1;
        var port = 8080;
        KVService storage = new KVServiceFactorySimple("storage", null, replicas).create(port);
        storage.start();
        log.info("Server started on port {}", port);
        Runtime.getRuntime().addShutdownHook(new Thread(storage::stop));
    }
}
