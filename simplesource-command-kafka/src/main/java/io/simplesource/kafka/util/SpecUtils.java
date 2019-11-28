package io.simplesource.kafka.util;

import io.simplesource.kafka.api.AggregateResources;
import io.simplesource.kafka.spec.AggregateSpec;
import io.simplesource.kafka.spec.CommandSpec;

public class SpecUtils {
    public static <K, C> CommandSpec<K, C> getCommandSpec(AggregateSpec<K, C, ?, ?> aSpec, String clientId) {
        return new CommandSpec<>(
                aSpec.aggregateName(),
                clientId,
                aSpec.serialization().resourceNamingStrategy(),
                aSpec.serialization().serdes(),
                aSpec.generation().stateStoreSpec(),
                aSpec.generation().topicConfig().get(AggregateResources.TopicEntity.COMMAND_RESPONSE));
    }
}
