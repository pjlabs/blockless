package io.github.pjlabs.blockless;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(10)
class LazyHelpingLifecycleTest {
  @Test
  void terminationWaitsForQueuedCancellationHooksAndLaterFutures() throws Exception {
    final var started = new CountDownLatch(1);
    final var holdWorker = new CountDownLatch(1);
    final var hookEntered = new CountDownLatch(1);
    final var releaseHook = new CountDownLatch(1);
    final var shutdownReturned = new CompletableFuture<Void>();
    final var executor = LazyHelpingExecutor.newFixedThreadPool(1);
    Thread stopper = null;
    try {
      executor.submit(
          () -> {
            started.countDown();
            holdWorker.await();
            return null;
          });
      assertTrue(started.await(5, TimeUnit.SECONDS));
      final var first =
          new FutureTask<Void>(() -> null) {
            @Override
            protected void done() {
              hookEntered.countDown();
              awaitUninterruptibly(releaseHook);
            }
          };
      final var second = new FutureTask<Void>(() -> null);
      executor.execute(first);
      executor.execute(second);
      stopper =
          Thread.ofPlatform()
              .start(
                  () -> {
                    try {
                      executor.shutdownNow();
                      shutdownReturned.complete(null);
                    } catch (Throwable failure) {
                      shutdownReturned.completeExceptionally(failure);
                    }
                  });
      assertTrue(hookEntered.await(5, TimeUnit.SECONDS));
      assertFalse(executor.awaitTermination(50, TimeUnit.MILLISECONDS));
      assertFalse(executor.isTerminated());
      assertFalse(second.isDone());
      releaseHook.countDown();
      shutdownReturned.get(5, TimeUnit.SECONDS);
      assertTrue(first.isCancelled());
      assertTrue(second.isCancelled());
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    } finally {
      releaseHook.countDown();
      holdWorker.countDown();
      if (stopper != null) stopper.join(5000);
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void shutdownNowReturnsTasksClaimedByAWorkerBeforeTheirBodiesStart() throws Exception {
    final var claimed = new CountDownLatch(1);
    final var releaseWorker = new CountDownLatch(1);
    final var executions = new AtomicInteger();
    final ThreadFactory factory =
        runner ->
            Thread.ofPlatform()
                .unstarted(
                    () -> {
                      claimed.countDown();
                      awaitUninterruptibly(releaseWorker);
                      runner.run();
                    });
    final var executor = LazyHelpingExecutor.newFixedThreadPool(1, factory);
    final Runnable task = executions::incrementAndGet;
    try {
      executor.execute(task);
      assertTrue(claimed.await(5, TimeUnit.SECONDS));
      final var queued = executor.submit(() -> "queued");
      final var abandoned = executor.shutdownNow();
      assertTrue(abandoned.contains(task));
      assertTrue(abandoned.contains(queued));
      assertTrue(queued.isCancelled());
      releaseWorker.countDown();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
      assertEquals(0, executions.get());
    } finally {
      releaseWorker.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void gracefulShutdownDoesNotNeedAnotherThreadFromTheFactory() throws Exception {
    final var attempts = new AtomicInteger();
    final var started = new CountDownLatch(1);
    final var releaseWorker = new CountDownLatch(1);
    final ThreadFactory factory =
        task -> {
          if (attempts.incrementAndGet() == 2) {
            throw new IllegalStateException("second worker unavailable");
          }
          return Thread.ofPlatform().unstarted(task);
        };
    final var executor = LazyHelpingExecutor.newFixedThreadPool(2, factory);
    try {
      final var task =
          executor.submit(
              () -> {
                started.countDown();
                releaseWorker.await();
                return "done";
              });
      assertTrue(started.await(5, TimeUnit.SECONDS));
      executor.shutdown();
      releaseWorker.countDown();
      assertEquals("done", task.get(5, TimeUnit.SECONDS));
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
      assertEquals(1, attempts.get());
    } finally {
      releaseWorker.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  private static void awaitUninterruptibly(CountDownLatch latch) {
    var interrupted = false;
    while (true) {
      try {
        latch.await();
        break;
      } catch (InterruptedException ignored) {
        interrupted = true;
      }
    }
    if (interrupted) Thread.currentThread().interrupt();
  }
}
