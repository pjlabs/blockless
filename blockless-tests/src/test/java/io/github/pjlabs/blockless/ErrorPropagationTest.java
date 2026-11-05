package io.github.pjlabs.blockless;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ErrorPropagationTest {

  @Nested
  class WithoutBlockless {
    @Test
    void joiningWorkerDoesNotPropagateItsError() throws InterruptedException {
      final var original = new AssertionError("worker failed");
      final var result = new AtomicReference<String>();
      final var uncaught = new AtomicReference<Throwable>();
      final Supplier<String> task =
          () -> {
            throw original;
          };
      final var worker =
          Thread.ofVirtual()
              .unstarted(() -> result.set(task.get()));
      worker.setUncaughtExceptionHandler((thread, failure) -> uncaught.set(failure));
      worker.start();
      worker.join();

      assertSame(original, uncaught.get());
      assertNull(result.get());
    }
  }

  @Nested
  class WithBlockless {
    @Test
    void callablePropagatesOriginalError() {
      final var original = new AssertionError("worker failed");
      final Callable<String> callable =
          () -> {
            throw original;
          };

      assertSame(original, assertThrows(AssertionError.class, () -> Blockless.get(callable)));
    }

    @Test
    void completionStagePropagatesOriginalError() {
      final var original = new AssertionError("stage failed");
      final var stage = new CompletableFuture<String>();
      stage.completeExceptionally(original);

      assertSame(original, assertThrows(AssertionError.class, () -> Blockless.get(stage)));
    }

    @Test
    void parallelPropagatesErrorsAndCollectsThemInEither() {
      for (final var parallel :
          List.of(Parallel.create(), Parallel.create().withTimeout(Duration.ofSeconds(30)))) {
        final var original = new AssertionError("task failed");
        assertSame(
            original,
            assertThrows(
                AssertionError.class,
                () ->
                    parallel
                        .async(
                            () -> {
                              throw original;
                            })
                        .get()));
        assertSame(
            original,
            assertThrows(
                AssertionError.class,
                () ->
                    parallel.map(
                        List.of(1),
                        item -> {
                          throw original;
                        })));
        final var results =
            parallel.toEither(
                List.of(1),
                item -> {
                  throw original;
                });
        assertSame(original, results.get(0).failure());
      }
    }

    @Test
    void callableStillPreservesRuntimeExceptionsAndWrapsCheckedExceptions() {
      final var runtime = new IllegalArgumentException("runtime");
      final Callable<String> unchecked =
          () -> {
            throw runtime;
          };
      assertSame(runtime, assertThrows(IllegalArgumentException.class, () -> Blockless.get(unchecked)));

      final var checked = new Exception("checked");
      final Callable<String> callable =
          () -> {
            throw checked;
          };
      final var wrapped = assertThrows(RuntimeException.class, () -> Blockless.get(callable));
      assertSame(checked, wrapped.getCause());
    }
  }
}
