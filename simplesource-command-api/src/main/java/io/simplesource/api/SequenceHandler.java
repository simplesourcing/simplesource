package io.simplesource.api;

import io.simplesource.data.Reason;
import io.simplesource.data.Sequence;

import java.util.Optional;

@FunctionalInterface
public interface SequenceHandler<K, C, A> {
    Optional<Reason<CommandAPI.CommandError>> rejectIfError(K key,
                                                            Sequence expectedSeq,
                                                            Sequence currentSeq,
                                                            A currentAggregate,
                                                            C command);
}
