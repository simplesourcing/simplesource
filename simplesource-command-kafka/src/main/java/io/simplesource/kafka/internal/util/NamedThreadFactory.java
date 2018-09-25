package io.simplesource.kafka.internal.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public final class NamedThreadFactory implements ThreadFactory {
    private final String name;
    private final AtomicInteger threadCount = new AtomicInteger();
    private final ThreadFactory backingThreadFactory = Executors.defaultThreadFactory();

    public NamedThreadFactory(final String name) {
        this.name = name;
    }

    @Override
    public Thread newThread(final Runnable runnable) {
        final Thread thread = backingThreadFactory.newThread(runnable);
        thread.setName(name + '-' + threadCount.incrementAndGet());
        return thread;
    }
}
