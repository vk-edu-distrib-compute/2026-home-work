package company.vk.edu.distrib.compute.glekoz.replica.records;

public record ProxyResult(int statusCode, byte[] body, boolean isSuccess) {
    public static ProxyResult success(int statusCode, byte[] body) {
        return new ProxyResult(statusCode, body, true);
    }
    
    public static ProxyResult error(int statusCode) {
        return new ProxyResult(statusCode, null, false);
    }
    
    public static ProxyResult error(Exception e) {
        return new ProxyResult(500, null, false);
    }
}
