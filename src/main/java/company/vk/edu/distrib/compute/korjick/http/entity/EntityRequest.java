package company.vk.edu.distrib.compute.korjick.http.entity;

public record EntityRequest(String method, String id, byte[] body, boolean internalRequest) {
}
