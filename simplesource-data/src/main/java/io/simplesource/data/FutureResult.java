package io.simplesource.data;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Supplier;


/**
 * Represents an operation that calculates an {@link Result} asynchronously.
 *
 * @param <E> on failure there will be a NonEmptyList of Reason instances with an error value of this type.
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
                return Result.failure(f.apply(e), e);
            }
        }));
    }

    public static <E, T> FutureResult<E, T> ofFutureResult(final Future<Result<E, T>> future, final Function<Exception, E> f) {
        return new FutureResult<>(CompletableFuture.supplyAsync(() -> {
            try {
                return future.get();
            } catch (final InterruptedException | ExecutionException e) {
                return Result.failure(f.apply(e), e);
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

    public static <E, T> FutureResult<E, T> fail(final E error, final String msg) {
        return fail(Reason.of(error, msg));
    }

    @SafeVarargs
    public static <E, T> FutureResult<E, T> fail(final Reason<E> reason, final Reason<E>... reasons) {
        return new FutureResult<>(() -> Result.failure(reason, reasons));
    }

    public static <E, T> FutureResult<E, T> fail(final NonEmptyList<Reason<E> > nonEmptyList) {
        return new FutureResult<>(() -> Result.failure(nonEmptyList));
    }

    public static <E, T> FutureResult<E, T> raise(final E error, final RuntimeException ex) {
        return fail(Reason.of(error, ex));
    }

    // TEMP
    public Result<E, T> unsafePerform(final Function<Exception, E> f) {
        try {
            return run.get();
        } catch (final InterruptedException | ExecutionException e) {
            return Result.failure(f.apply(e), e);
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
            return Result.failure(f.apply(e), e);
        }
    }

    public CompletableFuture<Result<E, T>> future() {
        return run;
    }

    public Result<E, T> getOrElseReason(final E error, final String reason) {
        try {
            return run.handle((tResult, throwable) -> tResult != null ? tResult : Result.<E, T>failure(error, reason)).get();
        } catch (final InterruptedException | ExecutionException e) {
            return Result.failure(error, e);
        }
    }

    public <R> FutureResult<E, R> map(Function<T, R> f) {
        // CompletableFuture thenApply === map
        return new FutureResult<>(run.thenApply(r -> r.map(f)));
    }

    public  <R> Future<R> fold(Function<NonEmptyList<Reason<E>>, R> e, Function<T, R> f) {
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
