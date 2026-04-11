package company.vk.edu.distrib.compute;

import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.support.ParameterDeclarations;
import org.junit.platform.commons.util.ReflectionUtils;

import java.util.Set;
import java.util.stream.Stream;

import company.vk.edu.distrib.compute.ronshinvsevolod.RonshinRendezvousHashKVClusterFactory;
import company.vk.edu.distrib.compute.ronshinvsevolod.RonshinConsistentHashKVClusterFactory;

public class KVClusterFactoryArgumentsProvider implements ArgumentsProvider {
    private final Set<Class<? extends KVClusterFactory>> factories = Set.of(
        RonshinRendezvousHashKVClusterFactory.class,
        RonshinConsistentHashKVClusterFactory.class
    );

    @Override
    @NonNull
    public Stream<? extends Arguments> provideArguments(
            @NonNull ParameterDeclarations parameters,
            @NonNull ExtensionContext context
    ) {
        return factories.stream()
                .map(ReflectionUtils::newInstance)
                .map(Arguments::of);
    }
}
