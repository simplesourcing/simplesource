package io.simplesource.dsl;

import io.simplesource.api.Aggregator;

import java.util.HashMap;
import java.util.Map;

/**
 * A builder for creating an {@link Aggregator} that can handle multiple event types by adding
 * one or more single event aggregators.
 *
 * @param <E> all events generated for this aggregate
 * @param <A> the aggregate type
 */
public final class AggregatorBuilder<E, A> {

    public static <E, A> AggregatorBuilder<E, A> newBuilder() {
        return new AggregatorBuilder<>();
    }

    private final Map<Class<? extends E>, Aggregator<? extends E, A>> aggregators = new HashMap<>();


    private AggregatorBuilder() {
    }

    public <SE extends E> AggregatorBuilder<E, A> onEvent(final Class<SE> specificEventClass, final Aggregator<SE, A> ch) {
        aggregators.put(specificEventClass, ch);
        return this;
    }

    private Map<Class<? extends E>, Aggregator<? extends E, A>> getAggregators() {
        return new HashMap<>(aggregators);
    }

    public Aggregator<E, A> build() {
        // defensive copy
        final Map<Class<? extends E>, Aggregator<? extends E, A>> eh = getAggregators();

        return  (currentAggregate, event) -> {
            final Aggregator<E, A> eventHandler = (Aggregator<E, A>) eh.get(event.getClass());

            if (eventHandler == null) {
                throw new IllegalStateException(String.format("Unhandled event type: %s",
                        event.getClass().getSimpleName()));
            }
            return eventHandler.applyEvent(currentAggregate, event);
        };
    }
}
