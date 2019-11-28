package io.simplesource.kafka.internal.streams;

import io.simplesource.kafka.api.AggregateResources;
import io.simplesource.kafka.api.ResourceNamingStrategy;
import io.simplesource.kafka.internal.streams.topology.EventSourcedTopology;
import io.simplesource.kafka.internal.streams.topology.TopologyContext;
import io.simplesource.kafka.spec.AggregateSetSpec;
import io.simplesource.kafka.spec.TopicSpec;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.streams.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.simplesource.kafka.util.KafkaStreamsUtils.*;
import static java.util.Objects.nonNull;

public final class EventSourcedStreamsApp {
    private static final Logger logger = LoggerFactory.getLogger(EventSourcedStreamsApp.class);

    private final AggregateSetSpec aggregateSetSpec;
    private final AdminClient adminClient;

    private KafkaStreams streams = null;

    public EventSourcedStreamsApp(
            final AggregateSetSpec aggregateSetSpec
    ) {
        this.aggregateSetSpec = aggregateSetSpec;
        adminClient = AdminClient.create(aggregateSetSpec.kafkaConfig().adminClientConfig());
    }

    public synchronized void start() {
        if (nonNull(streams)) throw new IllegalStateException("Application already started");

        createTopics();
        final Topology topology = buildTopology();
        streams = startApp(topology);
        waitUntilStable(logger, streams);
    }

    public KafkaStreams getStreams() {
        return streams;
    }

    private void createTopics() {
        try {
            final Collection<AggregateResources.TopicEntity> topicEntities = EnumSet.allOf(AggregateResources.TopicEntity.class);

            final Set<String> brokerTopics = adminClient.listTopics().listings().get()
                    .stream()
                    .map(TopicListing::name)
                    .collect(Collectors.toSet());

            final Stream<KeyValue<String, TopicSpec>> requiredTopics = aggregateSetSpec.aggregateConfigMap()
                    .values()
                    .stream()
                    .flatMap(aggregate -> {
                        final ResourceNamingStrategy namingStrategy = aggregate.serialization().resourceNamingStrategy();
                        final Map<AggregateResources.TopicEntity, TopicSpec> topicConfig = aggregate.generation()
                                .topicConfig();
                        return topicEntities.stream().map(topicEntity ->
                                KeyValue.pair(
                                        namingStrategy.topicName(aggregate.aggregateName(), topicEntity.name()),
                                        topicConfig.get(topicEntity)
                                ));
                    });

            final List<NewTopic> topicsToCreate = requiredTopics
                    .filter(topicKV -> !brokerTopics.contains(topicKV.key))
                    .map(topicKV -> {
                        final String topicName = topicKV.key;
                        final TopicSpec topicSpec = topicKV.value;
                        final NewTopic newTopic = new NewTopic(topicName, topicSpec.partitionCount(), topicSpec.replicaCount());
                        newTopic.configs(topicSpec.config());
                        return newTopic;
                    })
                    .collect(Collectors.toList());

            final CreateTopicsOptions createTopicsOptions = new CreateTopicsOptions();
            createTopicsOptions.timeoutMs(15000);

            adminClient.createTopics(topicsToCreate, createTopicsOptions).all().get();
        } catch (final Exception e) {
            throw new RuntimeException("Failed to empty required topics", e);
        }
    }

    private Topology buildTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        aggregateSetSpec.aggregateConfigMap()
                .values()
                .forEach(aggregateSpec ->
                        EventSourcedTopology.addTopology(new TopologyContext<>(aggregateSpec), builder));
        return builder.build();
    }

    private KafkaStreams startApp(final Topology topology) {
        logger.info("Topology description {}", topology.describe());

        // empty and set state store directory
        new File(aggregateSetSpec.kafkaConfig().stateDir()).mkdirs();

        final KafkaStreams streams = new KafkaStreams(topology, aggregateSetSpec.kafkaConfig().streamsConfig());
        registerExceptionHandler(logger, streams);
        addShutdownHook(logger, streams);
        streams.start();

        return streams;
    }
}
