package io.github.pjlabs.blockless;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(10)
class SupplierWaitSupportTest {
  @Test
  void restoresThePreviousHandlerAfterNestedFailure() {
    final var stage = new CompletableFuture<String>();
    final var supplier = new ResultSupplier<>(stage::join, stage);
    final var outer = new IllegalStateException("outer");
    final var inner = new IllegalStateException("inner");
    SupplierWaitSupport.withHandler(
        pending -> {
          throw outer;
        },
        () -> {
          assertSame(
              inner,
              assertThrows(
                  IllegalStateException.class,
                  () ->
                      SupplierWaitSupport.withHandler(
                          pending -> {
                            throw inner;
                          },
                          supplier::get)));
          assertSame(outer, assertThrows(IllegalStateException.class, supplier::get));
          return null;
        });
  }

  @Test
  void rejectsAnUnfinishedReadWhenTheHandlerReturnsPrematurely() {
    final var stage = new CompletableFuture<String>();
    final var supplier = new ResultSupplier<>(stage::join, stage);
    assertThrows(
        IllegalStateException.class,
        () -> SupplierWaitSupport.withHandler(pending -> {}, supplier::get));
  }

  @Test
  void skipsTheHandlerForCompletedOrVirtualThreadReads() throws Exception {
    final var ready = CompletableFuture.completedFuture("ready");
    assertEquals(
        "ready",
        SupplierWaitSupport.withHandler(
            pending -> fail("completed read"), new ResultSupplier<>(ready::join, ready)::get));
    try (final var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      final var stage = new CompletableFuture<String>();
      final var supplier =
          new ResultSupplier<>(
              () -> {
                stage.complete("virtual");
                return stage.join();
              },
              stage);
      assertEquals(
          "virtual",
          executor
              .submit(
                  () ->
                      SupplierWaitSupport.withHandler(
                          pending -> fail("virtual read"), supplier::get))
              .get(5, TimeUnit.SECONDS));
    }
  }
}
