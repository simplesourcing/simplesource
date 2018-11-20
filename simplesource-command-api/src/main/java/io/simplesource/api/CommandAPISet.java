package io.simplesource.api;

public interface CommandAPISet {

    /**
     * Provide access to the CommandAPI for any of the aggregates managed by this instance
     *
     * @param aggregateName unique name of one of the aggregates
     *
     * @param <C> all commands for this aggregate
     * @param <K> the aggregate key
     * @return the aggregate matching the given name
     */
    <K, C> CommandAPI<K, C> getCommandAPI(String aggregateName);
}
