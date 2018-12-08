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
    private static final int COUNTDOWN_SLEEP_TIME = 500;
    private static final String DEFAULT_VALUE = "not exist value";
    private static final TestError FAILURE_COMMAND_ERROR_1 = new TestError(TestError.Reason.InternalError);
    private static final TestError FAILURE_COMMAND_ERROR_2 = new TestError(TestError.Reason.UnexpectedErrorCode);

    private static final Function<NonEmptyList<TestError>, List<TestError.Reason>> TO_ERROR_REASON_FUNC =
            rs -> rs.stream().map(TestError::getReason).collect(Collectors.toList());

    private CountDownLatch futureResultReturnSignal;

    @BeforeEach
    void setUp() {
        futureResultReturnSignal = new CountDownLatch(1);
    }

    @Test
    void mapShouldNotMapValueWhenFutureResultIsFailure() {
        FutureResult<TestError, Integer> futureResult = asynchronousFailure(futureResultReturnSignal,
                FAILURE_COMMAND_ERROR_1);

        triggerFutureResultReturnSignal();
        Result<TestError, String> result = getFutureResultValue(futureResult.map(Object::toString));

        assertThat(result.getOrElse(DEFAULT_VALUE)).isEqualTo(DEFAULT_VALUE);
        assertTrue(result.isFailure());
    }

    @Test
    void mapShouldMapValueWhenFutureResultIsSuccess() {
        FutureResult<TestError, Integer> futureResult = asynchronousSuccess(futureResultReturnSignal, 10);

        triggerFutureResultReturnSignal();
        Result<TestError, String> result = getFutureResultValue(futureResult.map(Object::toString));

        assertThat(result.getOrElse(DEFAULT_VALUE)).isEqualTo("10");
        assertTrue(result.isSuccess());

    }

    @Test
    void foldShouldMapIntValueToStringWhenFutureResultIsSuccess() {
        FutureResult<TestError, Integer> futureResult = asynchronousSuccess(futureResultReturnSignal, 10);

        triggerFutureResultReturnSignal();
        assertDoesNotThrow(() -> {
            String actualResult = futureResult.fold(null, Object::toString).get();
            assertThat(actualResult).isEqualTo("10");
        });
    }

    @Test
    void foldShouldConcatenateErrorReasonsWhenFutureResultIsFailure() {
        FutureResult<TestError, Integer> futureResult = asynchronousFailure(futureResultReturnSignal,
                FAILURE_COMMAND_ERROR_1, FAILURE_COMMAND_ERROR_2);

        triggerFutureResultReturnSignal();

        assertDoesNotThrow(() -> {
            List<TestError.Reason> actualResult = futureResult.fold(TO_ERROR_REASON_FUNC, null).get();

            assertThat(actualResult).containsOnly(TestError.Reason.InternalError, TestError.Reason.UnexpectedErrorCode);
        });
    }

    @Test
    void errorMapShouldChangeTheErrorType() {
        FutureResult<TestError, Integer> futureResult = asynchronousFailure(futureResultReturnSignal,
                FAILURE_COMMAND_ERROR_1, FAILURE_COMMAND_ERROR_2);

        triggerFutureResultReturnSignal();

        assertDoesNotThrow(() -> {
            Result<TestError.Reason, Integer> actualResult = getFutureResultValue(futureResult.errorMap(TestError::getReason));
            assertThat(actualResult.isFailure()).isTrue();
            assertThat( actualResult.failureReasons().get()).containsOnly(TestError.Reason.InternalError, TestError.Reason.UnexpectedErrorCode);
        });
    }

    @Test
    void errorMapShouldDoNothingOnSuccess() {
        FutureResult<TestError, Integer> futureResult = asynchronousSuccess(futureResultReturnSignal, 10);

        triggerFutureResultReturnSignal();
        Result<TestError.Reason, Integer> result = getFutureResultValue(futureResult.errorMap(TestError::getReason));
        assertThat(result.getOrElse(-1)).isEqualTo(10);
        assertTrue(result.isSuccess());
    }

    @Test
    void flatMapShouldMapIntValueToStringWhenFutureResultIsSuccess() {
        FutureResult<TestError, Integer> futureResult = asynchronousSuccess(futureResultReturnSignal, 10);

        triggerFutureResultReturnSignal();
        Result<TestError, String> result = getFutureResultValue(futureResult.flatMap(v -> FutureResult.of(v.toString())));
        assertThat(result.getOrElse(DEFAULT_VALUE)).isEqualTo("10");
        assertTrue(result.isSuccess());
    }


    @Test
    void flatMapShouldReturnFailureReasonsAsTheyAreWithoutMappingAndDoesNotMapValueWhenFutureResultIsFailure() {
        FutureResult<TestError, Integer> futureResult = asynchronousFailure(futureResultReturnSignal,
                FAILURE_COMMAND_ERROR_1, FAILURE_COMMAND_ERROR_2);

        triggerFutureResultReturnSignal();
        Result<TestError, String> result = getFutureResultValue(futureResult.flatMap(v -> FutureResult.of(v.toString())));
        assertThat(result.getOrElse(DEFAULT_VALUE)).isEqualTo(DEFAULT_VALUE);
        assertThat(result.failureReasons()).contains(NonEmptyList.of(FAILURE_COMMAND_ERROR_1, FAILURE_COMMAND_ERROR_2));
    }

    private void triggerFutureResultReturnSignal() {
        new Thread(() -> {
            try {
                Thread.sleep(COUNTDOWN_SLEEP_TIME + (int) (Math.random() * COUNTDOWN_SLEEP_TIME));
                futureResultReturnSignal.countDown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();
    }

    private static <T> FutureResult<TestError, T> asynchronousSuccess(CountDownLatch countDownLatch, T v) {
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

    private static <T> FutureResult<TestError, T> asynchronousFailure(CountDownLatch countDownLatch, TestError error,
                                                                      TestError... errors) {
        return FutureResult.ofSupplier(
                () -> {
                    try {
                        countDownLatch.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    return Result.failure(error, errors);
                }
        );
    }

    private static <E, T> Result<E, T> getFutureResultValue(FutureResult<E, T> futureResult) {
        return futureResult.future().join();
    }
}