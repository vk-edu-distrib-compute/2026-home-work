package company.vk.edu.distrib.compute.ce_fello;

@FunctionalInterface
interface CeFelloKeyDistributor {
    String ownerFor(String key);
}
