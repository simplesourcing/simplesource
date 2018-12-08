package io.simplesource.data;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Supplier;


/**
 * Represents an operation that calculates an {@link Result} asynchronously.
 *
 * @param <E> on failure there will be a NonEmptyList of error instances with an error value of this type.
 * @param <T> when successful there will be a contained value of this type.
 */
public final class FutureResult<E, T> {

    private final CompletableFuture<Result<E, T>> run;

    public static <E, T> FutureResult<E, T> ofCompletableFuture(final CompletableFuture<Result<E, T>> run) {
        return new FutureResult<>(run);
    }

    public static <E, T> FutureResult<E, T> ofFuture(final Future<T> run, final Function<Exception, E> f) {
        return new FutureResult<>(CompletableFuture.supplyAsync(() -> {
            try {
                return Result.success(run.get());
            } catch (final InterruptedException | ExecutionException e) {
                return Result.failure(f.apply(e));
            }
        }));
    }

    public static <E, T> FutureResult<E, T> ofFutureResult(final Future<Result<E, T>> future, final Function<Exception, E> f) {
        return new FutureResult<>(CompletableFuture.supplyAsync(() -> {
            try {
                return future.get();
            } catch (final InterruptedException | ExecutionException e) {
                return Result.failure(f.apply(e));
            }
        }));
    }

    public static <E, T> FutureResult<E, T> ofResult(final Result<E, T> result) {
        return new FutureResult<>(() -> result);
    }

    public static <E, T> FutureResult<E, T> ofSupplier(final Supplier<Result<E, T>> supplier) {
        return new FutureResult<>(supplier);
    }


    public static <E, T> FutureResult<E, T> of(final T t) {
        return new FutureResult<>(() -> Result.success(t));
    }

    @SafeVarargs
    public static <E, T> FutureResult<E, T> fail(final E error, final E... errors) {
        return new FutureResult<>(() -> Result.failure(error, errors));
    }

    public static <E, T> FutureResult<E, T> fail(final NonEmptyList<E> errors) {
        return new FutureResult<>(() -> Result.failure(errors));
    }

    // TEMP
    public Result<E, T> unsafePerform(final Function<Exception, E> f) {
        try {
            return run.get();
        } catch (final InterruptedException | ExecutionException e) {
            E error = f.apply(e);
            return Result.failure(error);
        }
    }

    private FutureResult(final CompletableFuture<Result<E, T>> run) {
        this.run = run;
    }

    private FutureResult(final Supplier<Result<E, T>> supplier) {
        run = CompletableFuture.supplyAsync(supplier);
    }

    public Result<E, T> getOrElse(final Supplier<Result<E, T>> resultSupplier, final Function<Exception, E> f) {
        try {
            return run.handle((tResult, throwable) -> tResult != null ? tResult : resultSupplier.get()).get();
        } catch (final InterruptedException | ExecutionException e) {
            return Result.failure(f.apply(e));
        }
    }

    public CompletableFuture<Result<E, T>> future() {
        return run;
    }

    public <R> FutureResult<E, R> map(Function<T, R> f) {
        // CompletableFuture thenApply === map
        return new FutureResult<>(run.thenApply(r -> r.map(f)));
    }

    public <F> FutureResult<F, T> errorMap(Function<E, F> f) {
        return new FutureResult<>(run.thenApply(r -> r.errorMap(f)));
    }

    public <R> CompletableFuture<R> fold(Function<NonEmptyList<E>, R> e, Function<T, R> f) {
        return run.thenApply(r -> r.fold(e, f));
    }

    public <R> FutureResult<E, R> flatMap(Function<T, FutureResult<E, R>> f) {
        // CompletableFuture thenCompose / thenComposeAsync === flatMap
        final CompletableFuture<Result<E, R>> future =
                run.thenComposeAsync(
                        (Result<E, T> r) ->
                                r.fold(
                                        reasons -> CompletableFuture.completedFuture(Result.failure(reasons)),
                                        value ->  f.apply(value).run
                                )
                );

        return new FutureResult<>(future);
    }
}
