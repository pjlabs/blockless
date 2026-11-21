package io.github.pjlabs.blockless;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.pjlabs.blockless.continuations.Continuations;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(15)
class SupplierCompletionTest {
  @Test
  void completionCallbacksCanReadEachSupplierWithoutWaitingForItsWorker() throws Exception {
    final var stage = new CompletableFuture<String>();
    final var sources =
        List.of(
            Blockless.supplier(stage),
            Blockless.supplier(stage::join),
            Continuations.supplier(Runnable::run, () -> Blockless.get(stage)));
    final var results =
        sources.stream()
            .map(
                source -> {
                  final var result = new CompletableFuture<String>();
                  ((AsyncSupplier<String>) source)
                      .onComplete(
                          () -> {
                            try {
                              result.complete(source.get());
                            } catch (Throwable failure) {
                              result.completeExceptionally(failure);
                            }
                          });
                  return result;
                })
            .toList();
    stage.complete("ready");
    for (final var result : results) {
      assertEquals("ready", result.get(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void racingBodyCompletionDoesNotLoseOrDuplicateResults() throws Exception {
    final var reads = new AtomicInteger();
    for (int i = 0; i < 20_000; i++) {
      final var result = Continuations.supplier(Runnable::run, reads::incrementAndGet);
      final var ready = new CompletableFuture<Integer>();
      result.onComplete(() -> ready.complete(result.get()));
      assertEquals(i + 1, ready.get(5, TimeUnit.SECONDS));
    }
    assertEquals(20_000, reads.get());
  }
}
