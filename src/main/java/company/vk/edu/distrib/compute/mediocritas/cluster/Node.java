package company.vk.edu.distrib.compute.mediocritas.cluster;

public record Node(String host, int httpPort, int grpcPort) {

    public String httpEndpoint() {
        return "http://" + host + ":" + httpPort;
    }

    public String grpcTarget() {
        return host + ":" + grpcPort;
    }
}
