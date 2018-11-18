package io.simplesource.kafka.dsl;

import io.simplesource.kafka.api.CommandSerdes;
import io.simplesource.kafka.api.ResourceNamingStrategy;
import io.simplesource.kafka.spec.CommandSpec;
import io.simplesource.kafka.spec.TopicSpec;
import io.simplesource.kafka.spec.WindowSpec;
import org.apache.kafka.common.config.TopicConfig;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public final class CommandApiBuilder<K, C> {
    private String name;
    private String clientId;
    private ResourceNamingStrategy resourceNamingStrategy;
    private CommandSerdes<K, C> commandSerdes;
    private TopicSpec outputTopicSpec;
    private WindowSpec commandResponseStoreSpec;

    public static <K, C, E, A> CommandApiBuilder<K, C> newBuilder() {
        return new CommandApiBuilder<>();
    }

    private CommandApiBuilder() {
        outputTopicSpec = defaultTopicConfig();
        commandResponseStoreSpec = new WindowSpec(3600);
    }

    public CommandApiBuilder<K, C> withName(final String name) {
        this.name = name;
        return this;
    }

    public CommandApiBuilder<K, C> withClientId(final String clientId) {
        this.clientId = clientId;
        return this;
    }

    public CommandApiBuilder<K, C> withResourceNamingStrategy(final ResourceNamingStrategy resourceNamingStrategy) {
        this.resourceNamingStrategy = resourceNamingStrategy;
        return this;
    }

    public CommandApiBuilder<K, C> withSerdes(final CommandSerdes<K, C> aggregateSerdes) {
        this.commandSerdes = commandSerdes;
        return this;
    }

    public CommandApiBuilder<K, C> withTopicSpec(final TopicSpec topicSpec) {
        this.outputTopicSpec = topicSpec;
        return this;
    }

    public CommandApiBuilder<K, C> withCommandResponseRetention(final long retentionInSeconds) {
        commandResponseStoreSpec = new WindowSpec(retentionInSeconds);
        return this;
    }

    public <SC extends C> CommandSpec<K, C> build() {
        requireNonNull(name, "No name for aggregate has been defined");
        requireNonNull(resourceNamingStrategy, "No resource naming strategy for aggregate has been defined");
        requireNonNull(outputTopicSpec, "No topic config for aggregate has been defined");
        if (clientId == null) {
            try {
                clientId = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                requireNonNull(clientId, "No client Id was defined, and host name could not be resolved");
            }
        }

        return new CommandSpec<>(name, clientId, resourceNamingStrategy, commandSerdes, commandResponseStoreSpec, outputTopicSpec);
    }

    private TopicSpec defaultTopicConfig() {
        final Map<String, String> commandResponseTopic = new HashMap<>();
        commandResponseTopic.put(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(TimeUnit.DAYS.toMillis(1)));
        return new TopicSpec(1, (short)1, commandResponseTopic);
    }
}
