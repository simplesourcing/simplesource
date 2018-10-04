package io.simplesource.data;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FutureResultTest {
    private static final int COUNTDOWN_SLEEP_TIME = 1000;
    private static final String DEFAULT_VALUE = "not exist value";
    private static final Reason<CommandError> FAILURE_REASON_1 = Reason.of(CommandError.InternalError, "Internal error");
    private static final Reason<CommandError> FAILURE_REASON_2 = Reason.of(CommandError.InvalidCommand, "Invalid command error");
    private static final Function<NonEmptyList<Reason<CommandError>>, List<CommandError>> CONCATENATE_ERROR_REASONS_FUNC =
            rs -> rs.stream().map(Reason::getError).collect(Collectors.toList());

    private CountDownLatch futureResultReturnSignal;

    @BeforeEach
    void setUp() {
        futureResultReturnSignal =  new CountDownLatch(1);
    }

    @Test
    void mapShouldNotMapValueWhenFutureResultIsFailure() {
        FutureResult<CommandError, Integer> futureResult = asynchronousFailure(futureResultReturnSignal,
                Reason.of(CommandError.InternalError, "Command error"));

        triggerFutureResultReturnSignal();
        Result<CommandError, String> result = getFutureResultValue(futureResult.map(Object::toString));

        assertThat(result.getOrElse(DEFAULT_VALUE)).isEqualTo(DEFAULT_VALUE);
        assertTrue(result.isFailure());
    }

    @Test
    void mapShouldMapValueWhenFutureResultIsSuccess() {
        FutureResult<CommandError, Integer> futureResult = asynchronousSuccess(futureResultReturnSignal,10);

        triggerFutureResultReturnSignal();
        Result<CommandError, String> result = getFutureResultValue(futureResult.map(Object::toString));

        assertThat(result.getOrElse(DEFAULT_VALUE)).isEqualTo("10");
        assertTrue(result.isSuccess());

    }

    @Test
    void foldShouldMapIntValueToStringWhenFutureResultIsSuccess() {
        FutureResult<CommandError, Integer> futureResult = asynchronousSuccess(futureResultReturnSignal,10);

        triggerFutureResultReturnSignal();
        assertDoesNotThrow(() -> {
            String actualResult = futureResult.fold(null, Object::toString).get();
            assertThat(actualResult).isEqualTo("10");
        });
    }

    @Test
    void foldShouldConcatenateErrorReasonsWhenFutureResultIsFailure() {
        FutureResult<CommandError, Integer> futureResult = asynchronousFailure(futureResultReturnSignal,
                FAILURE_REASON_1, FAILURE_REASON_2);

        triggerFutureResultReturnSignal();
        assertDoesNotThrow(() -> {
            List<CommandError> actualResult = futureResult.fold(CONCATENATE_ERROR_REASONS_FUNC, null).get();
            assertThat(actualResult).containsOnly(FAILURE_REASON_1.getError(), FAILURE_REASON_2.getError());
        });
    }

    @Test
    void flatMapShouldMapIntValueToStringWhenFutureResultIsSuccess() {
        FutureResult<CommandError, Integer> futureResult = asynchronousSuccess(futureResultReturnSignal,10);

        triggerFutureResultReturnSignal();
        Result<CommandError, String> result = getFutureResultValue(futureResult.flatMap(v -> FutureResult.of(v.toString())));
        assertThat(result.getOrElse(DEFAULT_VALUE)).isEqualTo("10");
        assertTrue(result.isSuccess());
    }


    @Test
    void flatMapShouldReturnFailureReasonsAsTheyAreWithoutMappingAndDoesNotMapValueWhenFutureResultIsFailure() {
        FutureResult<CommandError, Integer> futureResult = asynchronousFailure(futureResultReturnSignal,
                FAILURE_REASON_1, FAILURE_REASON_2);

        triggerFutureResultReturnSignal();
        Result<CommandError, String> result = getFutureResultValue(futureResult.flatMap(v -> FutureResult.of(v.toString())));
        assertThat(result.getOrElse(DEFAULT_VALUE)).isEqualTo(DEFAULT_VALUE);
        assertThat(result.failureReasons()).contains(NonEmptyList.of(FAILURE_REASON_1, FAILURE_REASON_2));
    }




    private void triggerFutureResultReturnSignal() {
        new Thread(() -> {
            try {
                Thread.sleep( COUNTDOWN_SLEEP_TIME + (int)(Math.random()*COUNTDOWN_SLEEP_TIME) );
                futureResultReturnSignal.countDown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
    }

    private static <T> FutureResult<CommandError, T> asynchronousSuccess(CountDownLatch countDownLatch, T v) {
        return FutureResult.ofSupplier(
                () -> {
                    try {
                        countDownLatch.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    return Result.success(v);
                }
        );
    }

    private static <T> FutureResult<CommandError, T> asynchronousFailure(CountDownLatch countDownLatch, Reason<CommandError> reason,
                                                                         Reason<CommandError>... reasons) {
        return FutureResult.ofSupplier(
                () -> {
                    try {
                        countDownLatch.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    return Result.failure(reason, reasons);
                }
        );
    }

    private static <T> Result<CommandError, T> getFutureResultValue(FutureResult<CommandError, T> futureResult) {
        return futureResult.future().join();
    }

    enum CommandError {
        InternalError,
        InvalidCommand
    }
}