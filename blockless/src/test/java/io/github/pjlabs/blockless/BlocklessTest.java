package io.github.pjlabs.blockless;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class BlocklessTest {

  @Test
  @Timeout(5)
  void interruptedStageWaiterDoesNotCorruptSharedOutcome() throws Exception {
    final var stage = new CompletableFuture<String>();
    final var supplier = Blockless.supplier(stage);
    assertInterruptedWaiterIsIsolated(supplier);
    stage.complete("done");
    assertEquals("done", supplier.get());
    assertEquals("done", supplier.get());
  }

  @Test
  @Timeout(5)
  void interruptedCallableWaiterDoesNotCorruptSharedOutcome() throws Exception {
    final var release = new CountDownLatch(1);
    final var supplier =
        Blockless.supplier(
            () -> {
              release.await();
              return "done";
            });
    try {
      assertInterruptedWaiterIsIsolated(supplier);
    } finally {
      release.countDown();
    }
    assertEquals("done", supplier.get());
    assertEquals("done", supplier.get());
  }

  private void assertInterruptedWaiterIsIsolated(Supplier<String> supplier) throws Exception {
    final var waiting = new CountDownLatch(1);
    final var result =
        new FutureTask<Void>(
            () -> {
              waiting.countDown();
              final var thrown = assertThrows(RuntimeException.class, supplier::get);
              assertInstanceOf(InterruptedException.class, thrown.getCause());
              assertTrue(Thread.currentThread().isInterrupted());
              return null;
            });
    final var waiter = Thread.ofVirtual().start(result);
    waiting.await();
    waiter.interrupt();
    result.get(2, TimeUnit.SECONDS);
  }

  @Test
  @Timeout(5)
  void callableErrorPublishesFailureAndReleasesWaiters() {
    final var error = new AssertionError("failed");
    final var supplier =
        Blockless.supplier(
            () -> {
              throw error;
            });
    assertSame(error, assertThrows(RuntimeException.class, supplier::get).getCause());
  }

  @Nested
  class GetCompletionStage {

    @Test
    void waitsForDelayedStage() {
      final var stage =
          CompletableFuture.supplyAsync(
              () -> "later", CompletableFuture.delayedExecutor(50, TimeUnit.MILLISECONDS));
      assertEquals("later", Blockless.get(stage));
    }

    @Test
    void handlesNullResult() {
      assertNull(Blockless.get(CompletableFuture.completedFuture(null)));
    }

    @Test
    void rethrowsRuntimeExceptionDirectly() {
      final var stage = CompletableFuture.failedFuture(new IllegalArgumentException("bad input"));
      final var thrown = assertThrows(IllegalArgumentException.class, () -> Blockless.get(stage));
      assertEquals("bad input", thrown.getMessage());
    }

    @Test
    void handlesAlreadyFailedStage() {
      final var stage = new CompletableFuture<String>();
      stage.completeExceptionally(new RuntimeException("already failed"));

      final var thrown = assertThrows(RuntimeException.class, () -> Blockless.get(stage));
      assertEquals("already failed", thrown.getMessage());
    }

    @Test
    void supplierDefersAndCanBeReused() {
      final var stage =
          CompletableFuture.supplyAsync(
              () -> "deferred", CompletableFuture.delayedExecutor(50, TimeUnit.MILLISECONDS));

      final var supplier = Blockless.supplier(stage);

      // blocks until complete, then returns same value on repeated calls
      assertEquals("deferred", supplier.get());
      assertEquals("deferred", supplier.get());
    }

    @Test
    void worksRegardlessOfCompletingExecutor() {
      // Stage completes on ForkJoinPool (default) — Blockless.get should
      // still work because it waits on its own latch, not the
      // stage's executor
      final var stage =
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  Thread.sleep(30);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
                return "from-forkjoin";
              });
      assertEquals("from-forkjoin", Blockless.get(stage));
    }
  }

  @Nested
  class GetCallable {

    @Test
    void executesOnVirtualThread() {
      assertTrue(
          Blockless.get(() -> Thread.currentThread().isVirtual()),
          "Blockless.get(Callable) must execute on a virtual thread");
    }

    @Test
    void waitsForBlockingCallable() {
      assertEquals(
          "later",
          Blockless.get(
              () -> {
                Thread.sleep(50);
                return "later";
              }));
    }

    @Test
    void rethrowsUncheckedExceptionDirectly() {
      final var thrown =
          assertThrows(
              IllegalStateException.class,
              () ->
                  Blockless.get(
                      () -> {
                        throw new IllegalStateException("broken");
                      }));
      assertEquals("broken", thrown.getMessage());
    }

    @Test
    void wrapsCheckedExceptionInRuntimeException() {
      final var thrown =
          assertThrows(
              RuntimeException.class,
              () ->
                  Blockless.get(
                      () -> {
                        throw new java.io.IOException("disk error");
                      }));
      assertInstanceOf(java.io.IOException.class, thrown.getCause());
    }

    @Test
    void supplierRethrowsUncheckedDirectly() {
      final var supplier =
          Blockless.supplier(
              () -> {
                throw new IllegalArgumentException("bad");
              });

      final var thrown = assertThrows(IllegalArgumentException.class, supplier::get);
      assertEquals("bad", thrown.getMessage());
    }
  }
}
