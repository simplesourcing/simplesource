package io.simplesource.kafka.internal.cluster;

import io.simplesource.data.CommandError;
import io.simplesource.data.CommandError.Reason;
import io.simplesource.data.Sequence;
import io.simplesource.data.NonEmptyList;
import io.simplesource.data.Result;
import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.*;

public final class CommandResponseTest {

    @Test
    public void serializationSuccess() {
            qt().forAll( longs().between(0L,Long.MAX_VALUE), arrays().ofIntegers(integers().allPositive()).withLengthBetween(1,100)).checkAssert( (l,ints) ->{
                Message cmdr = Message.response(l,
                        Result.success(
                                NonEmptyList.fromList(
                                        Stream.of(ints).map((j) -> Sequence.position((long)j)).collect(Collectors.toList())
                                )
                        )
                );
                assertEquals(cmdr, Message.fromByteArray(cmdr.toByteArray()));
            });
    }


    @Test
    public void serializationFailure() {
            qt().forAll(longs().between(0L, Long.MAX_VALUE),arrays().ofStrings(strings().basicLatinAlphabet().ofLengthBetween(0, 100)).withLengthBetween(1, 15)).checkAssert((l,strings) -> {
                Message cmdr =  Message.response(l,
                        Result.failure(
                                NonEmptyList.fromList(
                                        Stream.of(strings).map((s) -> CommandError.of(randomEnum(), s)).collect(Collectors.toList())
                                )
                        )
                );
                assertEquals(cmdr, Message.CommandResponse.fromByteArray(cmdr.toByteArray()));
            });

    }


    private Reason randomEnum () {
            Random random = new Random();
            return Reason.values()[random.nextInt(Reason.values().length)];
    }

}
