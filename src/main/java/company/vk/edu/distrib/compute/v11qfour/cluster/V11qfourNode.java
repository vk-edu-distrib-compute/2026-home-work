package company.vk.edu.distrib.compute.v11qfour.cluster;

public record V11qfourNode(String url) {
    public boolean isSelf(String currentUrl) {
        return this.url.equals(currentUrl);
    }
}
