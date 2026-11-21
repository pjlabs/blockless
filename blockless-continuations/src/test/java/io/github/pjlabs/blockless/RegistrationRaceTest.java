package io.github.pjlabs.blockless;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.github.pjlabs.blockless.continuations.Continuations;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(15)
class RegistrationRaceTest {
  @Test
  void completionDuringRegistrationTrampolinesWithoutRecursiveResumption() throws Exception {
    final var reads = new AtomicInteger();
    Continuations.run(
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
            })
        .toCompletableFuture()
        .get(5, TimeUnit.SECONDS);
    assertEquals(20_000, reads.get());
  }
}
