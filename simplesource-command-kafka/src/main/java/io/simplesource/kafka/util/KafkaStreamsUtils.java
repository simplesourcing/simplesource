package io.simplesource.kafka.util;

import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public final class KafkaStreamsUtils {

    public static void registerExceptionHandler(final Logger logger, final KafkaStreams streams) {
        streams.setUncaughtExceptionHandler(
                (Thread t, Throwable e) -> {
                    logger.error("Unhandled exception in " + t.getName() + ", exiting. {}", streams, e);
                    System.exit(1);
                }
        );
    }

    public static void addShutdownHook(final Logger logger, final KafkaStreams streams) {
        Runtime.getRuntime().addShutdownHook(
                new Thread(
                        () -> {
                            logger.info("Kafka Streams [{}] is shutting down", streams);
                            streams.close(15L, TimeUnit.SECONDS);
                        }
                )
        );
    }

    public static void waitUntilStable(
            final Logger logger,
            final KafkaStreams streams) {
        boolean done = false;
        do {
            logger.info("KafkaStreams now in state {} waiting until RUNNING for 5 seconds", streams.state());
            try {
                while (!Objects.equals(streams.state(), KafkaStreams.State.RUNNING)) {
                    Thread.sleep(1000L);
                }
                long inRunning = System.currentTimeMillis();
                while (Objects.equals(streams.state(), KafkaStreams.State.RUNNING) &&
                        System.currentTimeMillis() - inRunning < 5000L) {
                    Thread.sleep(1000L);
                }
                if (Objects.equals(streams.state(), KafkaStreams.State.RUNNING)) {
                    done = true;
                }
            } catch (InterruptedException e) {
                logger.warn("wait was interrupted", e);
            }
        } while (!done);
        logger.info("Streams app stable for 5 seconds. Considered up.");
    }

    public static void waitForShutdown(final Logger logger, final KafkaStreams streams) {
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(latch::countDown));
        try {
            latch.await();
        } catch (Exception e) {
            logger.warn("Error waiting for shutdown: {}", e);
            System.exit(1);
        }
    }

}
