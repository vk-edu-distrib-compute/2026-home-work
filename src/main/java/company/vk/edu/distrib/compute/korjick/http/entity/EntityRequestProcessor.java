package company.vk.edu.distrib.compute.korjick.http.entity;

import java.io.IOException;

@FunctionalInterface
public interface EntityRequestProcessor {
    EntityResponse process(EntityRequest request) throws IOException;
}
