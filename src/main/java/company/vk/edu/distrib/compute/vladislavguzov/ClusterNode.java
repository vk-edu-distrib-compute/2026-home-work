package company.vk.edu.distrib.compute.vladislavguzov;

import com.sun.net.httpserver.HttpServer;
import company.vk.edu.distrib.compute.Dao;

public record ClusterNode(int port, HttpServer server, Dao<byte[]> dao) {

    public String getUrl() {
        return "http://localhost:" + port;
    }
}
