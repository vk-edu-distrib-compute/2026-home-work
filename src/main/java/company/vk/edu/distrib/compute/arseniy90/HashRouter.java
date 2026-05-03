package company.vk.edu.distrib.compute.arseniy90;

@SuppressWarnings("PMD.ImplicitFunctionalInterface")
public interface HashRouter {
    // Возврщает ноду кластера
    String getEndpoint(String key);
}
