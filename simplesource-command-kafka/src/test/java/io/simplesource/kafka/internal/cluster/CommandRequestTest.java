package io.simplesource.kafka.internal.cluster;

import org.apache.kafka.streams.state.HostInfo;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;

import static org.quicktheories.QuickTheory.qt;
import static org.quicktheories.generators.SourceDSL.*;
import static org.junit.jupiter.api.Assertions.assertEquals;


public final class CommandRequestTest {

    @Test
    public void serialization() {
        qt().forAll(
                strings().basicLatinAlphabet().ofLengthBetween(0, 100),
                longs().between(1, Long.MAX_VALUE),
                integers().between(0,Integer.MAX_VALUE))
                .checkAssert((string,l,i) -> {
            Message cmd = Message.request(l, new HostInfo(string,i),string, UUID.randomUUID(), Duration.ofMillis(l));
            assertEquals(cmd, cmd.fromByteArray(cmd.toByteArray()));
        });
    }

}
