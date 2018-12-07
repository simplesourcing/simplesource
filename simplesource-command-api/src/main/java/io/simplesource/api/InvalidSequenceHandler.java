package io.simplesource.api;

import io.simplesource.data.Sequence;

import java.util.Optional;

/**
 * This functional for accepting or rejecting commands executed against an out of date view.
 * An incoming command has a <code>readSequence</code>, which represents the clients view of the aggregate version.
 * This sequence is checked against the sequence of the internally maintained aggregate version, which is guaranteed to be the latest.
 * The <code>shouldReject</code> method will be invoked if and only if these versions do not agree
 *
 * Normally a client should not have to explicitly implement this interface. There are standard implementations that cover most use cases
 * These are provided by choosing the appropriate <code>InvalidSequenceStrategy</code> when using the <code>AggregateBuilder</code> DSL.
 *
 * @param <K> the aggregate key type
 * @param <C> the command type
 * @param <A> the aggregate type
 */
@FunctionalInterface
public interface InvalidSequenceHandler<K, C, A> {
    /**
     * The sequence handler is responsible for accepting or rejecting commands executed against an out of date view
     * If expectedSeq == currentSeq this handler will not be invoked
     * If this handler is invoked, it means the is executing a command against an old version of the aggregate
     *
     * @param key the aggregate key
     * @param expectedSeq the current sequence number of the aggregate (i.e. the lastest)
     * @param currentSeq the sequence number passed in the command request
     * @param currentAggregate the current aggregate value
     * @param command the command currently being invoked
     *
     * @return If the command should be rejected, return <code>Optional</code> with a reason. If the command should still be invoked, return <code>Optional.empty</code>
     */
    Optional<CommandError> shouldReject(
            K key,
            Sequence expectedSeq,
            Sequence currentSeq,
            A currentAggregate,
            C command);
    }
