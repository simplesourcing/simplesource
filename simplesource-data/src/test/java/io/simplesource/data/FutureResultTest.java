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
    private static final io.simplesource.data.CommandError FAILURE_COMMAND_ERROR_1 =
            io.simplesource.data.CommandError.of(io.simplesource.data.CommandError.Reason.valueOf(BusinessErrorType.InternalError.name()),
                    "Internal error");
    private static final io.simplesource.data.CommandError FAILURE_COMMAND_ERROR_2 =
            io.simplesource.data.CommandError.of(io.simplesource.data.CommandError.Reason.valueOf(BusinessErrorType.InvalidCommand.name()),
                    "Invalid command error");

    private static final Function<NonEmptyList<io.simplesource.data.CommandError>, List<BusinessErrorType>> TO_BUSINESS_ERROR_TYPE_FUNC =
            rs -> rs.stream().map(FutureResultTest::toBusinessError).collect(Collectors.toList());

    private CountDownLatch futureResultReturnSignal;

    @BeforeEach
    void setUp() {
        futureResultReturnSignal =  new CountDownLatch(1);
    }

    @Test
    void mapShouldNotMapValueWhenFutureResultIsFailure() {
        FutureResult<io.simplesource.data.CommandError, Integer> futureResult = asynchronousFailure(futureResultReturnSignal,
                FAILURE_COMMAND_ERROR_1);

        triggerFutureResultReturnSignal();
        Result<io.simplesource.data.CommandError, String> result = getFutureResultValue(futureResult.map(Object::toString));

        assertThat(result.getOrElse(DEFAULT_VALUE)).isEqualTo(DEFAULT_VALUE);
        assertTrue(result.isFailure());
    }

    @Test
    void mapShouldMapValueWhenFutureResultIsSuccess() {
        FutureResult<io.simplesource.data.CommandError, Integer> futureResult = asynchronousSuccess(futureResultReturnSignal,10);

        triggerFutureResultReturnSignal();
        Result<io.simplesource.data.CommandError, String> result = getFutureResultValue(futureResult.map(Object::toString));

        assertThat(result.getOrElse(DEFAULT_VALUE)).isEqualTo("10");
        assertTrue(result.isSuccess());

    }

    @Test
    void foldShouldMapIntValueToStringWhenFutureResultIsSuccess() {
        FutureResult<io.simplesource.data.CommandError, Integer> futureResult = asynchronousSuccess(futureResultReturnSignal,10);

        triggerFutureResultReturnSignal();
        assertDoesNotThrow(() -> {
            String actualResult = futureResult.fold(null, Object::toString).get();
            assertThat(actualResult).isEqualTo("10");
        });
    }

    @Test
    void foldShouldConcatenateErrorReasonsWhenFutureResultIsFailure() {
        FutureResult<io.simplesource.data.CommandError, Integer> futureResult = asynchronousFailure(futureResultReturnSignal,
                FAILURE_COMMAND_ERROR_1, FAILURE_COMMAND_ERROR_2);

        triggerFutureResultReturnSignal();
        assertDoesNotThrow(() -> {
            List<BusinessErrorType> actualResult = futureResult.fold(TO_BUSINESS_ERROR_TYPE_FUNC, null).get();
            assertThat(actualResult).containsOnly(toBusinessError(FAILURE_COMMAND_ERROR_1),
                    toBusinessError(FAILURE_COMMAND_ERROR_2));
        });
    }

    @Test
    void flatMapShouldMapIntValueToStringWhenFutureResultIsSuccess() {
        FutureResult<io.simplesource.data.CommandError, Integer> futureResult = asynchronousSuccess(futureResultReturnSignal,10);

        triggerFutureResultReturnSignal();
        Result<io.simplesource.data.CommandError, String> result = getFutureResultValue(futureResult.flatMap(v -> FutureResult.of(v.toString())));
        assertThat(result.getOrElse(DEFAULT_VALUE)).isEqualTo("10");
        assertTrue(result.isSuccess());
    }


    @Test
    void flatMapShouldReturnFailureReasonsAsTheyAreWithoutMappingAndDoesNotMapValueWhenFutureResultIsFailure() {
        FutureResult<io.simplesource.data.CommandError, Integer> futureResult = asynchronousFailure(futureResultReturnSignal,
                FAILURE_COMMAND_ERROR_1, FAILURE_COMMAND_ERROR_2);

        triggerFutureResultReturnSignal();
        Result<io.simplesource.data.CommandError, String> result = getFutureResultValue(futureResult.flatMap(v -> FutureResult.of(v.toString())));
        assertThat(result.getOrElse(DEFAULT_VALUE)).isEqualTo(DEFAULT_VALUE);
        assertThat(result.failureReasons()).contains(NonEmptyList.of(FAILURE_COMMAND_ERROR_1, FAILURE_COMMAND_ERROR_2));
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

    private static <T> FutureResult<io.simplesource.data.CommandError, T> asynchronousSuccess(CountDownLatch countDownLatch, T v) {
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

    private static <T> FutureResult<io.simplesource.data.CommandError, T> asynchronousFailure(CountDownLatch countDownLatch, io.simplesource.data.CommandError commandError,
                                                                                              io.simplesource.data.CommandError... commandErrors) {
        return FutureResult.ofSupplier(
                () -> {
                    try {
                        countDownLatch.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    return Result.failure(commandError, commandErrors);
                }
        );
    }

    private static <T> Result<io.simplesource.data.CommandError, T> getFutureResultValue(FutureResult<io.simplesource.data.CommandError, T> futureResult) {
        return futureResult.future().join();
    }

    enum BusinessErrorType {
        InternalError,
        InvalidCommand
    }

    private static BusinessErrorType toBusinessError(CommandError error) {
        return BusinessErrorType.valueOf(error.getReason().name());
    }
}