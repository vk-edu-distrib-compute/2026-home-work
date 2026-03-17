import company.vk.edu.distrib.compute.KVService;
import company.vk.edu.distrib.compute.KVServiceFactory;
import org.slf4j.LoggerFactory;


@SuppressWarnings("DefaultPackage")
void main() throws IOException {
    var log = LoggerFactory.getLogger("server");
    var port = 8080;
    KVService storage = KVServiceFactory.create(port);
    storage.start();
    log.info("Server started on port {}", port);
    Runtime.getRuntime().addShutdownHook(new Thread(storage::stop));
}
