package company.vk.edu.distrib.compute.shuuuurik;

/**
 * Пара портов одного узла кластера: HTTP (внешний) и gRPC (внутренний).
 */
public record NodePorts(int httpPort, int grpcPort) {
}
