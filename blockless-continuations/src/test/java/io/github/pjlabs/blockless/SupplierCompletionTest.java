package io.github.pjlabs.blockless;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.github.pjlabs.blockless.continuations.Continuations;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
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
            Parallel.create().async(stage::join),
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
  void completionDuringRegistrationTrampolinesWithoutRecursiveResumption() throws Exception {
    final var reads = new AtomicInteger();
    final var result =
        Continuations.supplier(
            Runnable::run,
            () -> {
              for (int i = 0; i < 20_000; i++) {
                final var ready =
                    new CompletableFuture<Void>() {
                      @Override
                      public CompletableFuture<Void> whenComplete(
                          BiConsumer<? super Void, ? super Throwable> action) {
                        // Finish after the read suspended, immediately before registering its
                        // callback.
                        complete(null);
                        return super.whenComplete(action);
                      }
                    };
                final var supplier = new ResultSupplier<>(reads::incrementAndGet, ready);
                assertEquals(i + 1, supplier.get());
              }
              return null;
            });
    assertNull(result.get());
    assertEquals(20_000, reads.get());
  }
}
