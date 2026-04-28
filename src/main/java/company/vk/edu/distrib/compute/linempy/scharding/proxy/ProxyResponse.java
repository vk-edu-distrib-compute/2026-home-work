package company.vk.edu.distrib.compute.linempy.scharding.proxy;

public record ProxyResponse(int statusCode, byte[] body) {
    public static ProxyResponse of(int statusCode, byte[] body) {
        return new ProxyResponse(statusCode, body);
    }

    public static ProxyResponse error() {
        return new ProxyResponse(503, new byte[0]);
    }
}