package io.github.pjlabs.blockless;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

@Timeout(10)
class LazyHelpingExecutorTest {
  private static final ThreadLocal<String> CONTEXT = new ThreadLocal<>();
  private static final InheritableThreadLocal<String> INHERITED = new InheritableThreadLocal<>();

  @Test
  void usesOrdinarySuppliersAndReturnsTheirValues() throws Exception {
    try (final var executor = LazyHelpingExecutor.newFixedThreadPool(1)) {
      final var user =
          executor.submit(
              () -> {
                final var name = Blockless.supplier(() -> "Ada");
                final var permissions = Blockless.supplier(() -> "read");
                return name.get() + ":" + permissions.get();
              });
      assertEquals("Ada:read", user.get(5, TimeUnit.SECONDS));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1})
  void rejectsNonPositiveHelpedLimits(int maxHelped) {
    assertThrows(
        IllegalArgumentException.class, () -> LazyHelpingExecutor.newFixedThreadPool(1, maxHelped));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            LazyHelpingExecutor.newFixedThreadPool(1, maxHelped, Executors.defaultThreadFactory()));
  }

  @Nested
  class WithoutBlockless {
    @Test
    void ordinaryPlatformWorkerCannotDispatchWhileItsGetIsPending() throws Exception {
      final var atGet = new CountDownLatch(1);
      final var release = new CountDownLatch(1);
      try (final var executor = Executors.newFixedThreadPool(1)) {
        try {
          final var caller =
              executor.submit(
                  () -> {
                    final var user =
                        Blockless.supplier(
                            () -> {
                              release.await();
                              return "Ada";
                            });
                    atGet.countDown();
                    return user.get();
                  });
          assertTrue(atGet.await(5, TimeUnit.SECONDS));
          final var queued = executor.submit(() -> "queued");
          assertThrows(TimeoutException.class, () -> queued.get(50, TimeUnit.MILLISECONDS));
          release.countDown();
          assertEquals("Ada", caller.get(5, TimeUnit.SECONDS));
          assertEquals("queued", queued.get(5, TimeUnit.SECONDS));
        } finally {
          release.countDown();
        }
      }
    }
  }

  @Nested
  class WithBlockless {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void helpsPendingBlocklessAndParallelSuppliersWithoutMovingTheCaller(boolean useParallel)
        throws Exception {
      final var atGet = new CountDownLatch(1);
      final var release = new CountDownLatch(1);
      final var platformCreated = new AtomicInteger();
      final var lock = new ReentrantLock();
      final var originalLoader = Thread.currentThread().getContextClassLoader();
      final var requestLoader = new ClassLoader(originalLoader) {};
      final ThreadFactory factory =
          task ->
              Thread.ofPlatform()
                  .name("request-" + platformCreated.incrementAndGet())
                  .unstarted(task);
      CONTEXT.set("submitter");
      INHERITED.set("submitter");
      try (final var executor = LazyHelpingExecutor.newFixedThreadPool(1, factory)) {
        try {
          final var caller =
              executor.submit(
                  () -> {
                    final var original = Thread.currentThread();
                    assertFalse(original.isVirtual());
                    CONTEXT.set("request");
                    INHERITED.set("request");
                    original.setContextClassLoader(requestLoader);
                    lock.lock();
                    try {
                      final Supplier<String> io =
                          () -> {
                            await(release);
                            return "Ada";
                          };
                      final var user =
                          useParallel ? Parallel.create().async(io) : Blockless.supplier(io::get);
                      atGet.countDown();
                      final var value = user.get();
                      assertSame(original, Thread.currentThread());
                      assertEquals("request", CONTEXT.get());
                      assertEquals("request", INHERITED.get());
                      assertSame(requestLoader, original.getContextClassLoader());
                      assertTrue(lock.isHeldByCurrentThread());
                      return value;
                    } finally {
                      lock.unlock();
                      original.setContextClassLoader(originalLoader);
                      CONTEXT.remove();
                      INHERITED.remove();
                    }
                  });
          assertTrue(atGet.await(5, TimeUnit.SECONDS));
          executor
              .submit(
                  () -> {
                    assertTrue(Thread.currentThread().isVirtual());
                    assertNull(CONTEXT.get());
                    assertEquals("submitter", INHERITED.get());
                    assertSame(originalLoader, Thread.currentThread().getContextClassLoader());
                    final var acquired = lock.tryLock();
                    if (acquired) lock.unlock();
                    assertFalse(
                        acquired, "Helped work must not inherit the caller's lock ownership");
                  })
              .get(5, TimeUnit.SECONDS);
          assertFalse(caller.isDone(), "The actual result is still pending");
          release.countDown();
          assertEquals("Ada", caller.get(5, TimeUnit.SECONDS));
          assertEquals(1, platformCreated.get());
          assertEquals("submitter", CONTEXT.get());
          assertEquals("submitter", INHERITED.get());
        } finally {
          release.countDown();
        }
      } finally {
        CONTEXT.remove();
        INHERITED.remove();
      }
    }

    @Test
    void capsHelpedConcurrencyAndStartsQueuedTasksAsPermitsFreeUp() throws Exception {
      final var stage = new CompletableFuture<String>();
      final var user = Blockless.supplier(stage);
      final var started = new Semaphore(0);
      final var finish = new Semaphore(0);
      final var running = new AtomicInteger();
      final var peak = new AtomicInteger();
      try (final var executor = LazyHelpingExecutor.newFixedThreadPool(1, 2)) {
        try {
          final var caller = executor.submit(user::get);
          final var queued = new ArrayList<Future<Void>>();
          for (var i = 0; i < 5; i++) {
            queued.add(
                executor.submit(
                    () -> {
                      assertTrue(Thread.currentThread().isVirtual());
                      peak.accumulateAndGet(running.incrementAndGet(), Math::max);
                      try {
                        started.release();
                        finish.acquire();
                        return null;
                      } finally {
                        running.decrementAndGet();
                      }
                    }));
          }
          assertTrue(started.tryAcquire(2, 5, TimeUnit.SECONDS));
          assertFalse(started.tryAcquire(50, TimeUnit.MILLISECONDS));
          for (var i = 0; i < 3; i++) {
            finish.release();
            assertTrue(started.tryAcquire(5, TimeUnit.SECONDS));
          }
          finish.release(2);
          for (final var task : queued) task.get(5, TimeUnit.SECONDS);
          assertEquals(2, peak.get());
          assertFalse(caller.isDone());
          stage.complete("Ada");
          assertEquals("Ada", caller.get(5, TimeUnit.SECONDS));
        } finally {
          finish.release(5);
          stage.complete("Ada");
        }
      }
    }

    @Test
    void returnsPermitWhenHelpedTaskThrows() throws Exception {
      final var stage = new CompletableFuture<String>();
      final var user = Blockless.supplier(stage);
      final var releaseFailure = new CountDownLatch(1);
      final var uncaught = new CompletableFuture<Throwable>();
      final var failure = new IllegalStateException("helped task failed");
      try (final var executor = LazyHelpingExecutor.newFixedThreadPool(1, 1)) {
        try {
          final var caller = executor.submit(user::get);
          executor.execute(
              () -> {
                Thread.currentThread()
                    .setUncaughtExceptionHandler((thread, thrown) -> uncaught.complete(thrown));
                await(releaseFailure);
                throw failure;
              });
          final var next = executor.submit(() -> Thread.currentThread().isVirtual());
          releaseFailure.countDown();
          assertSame(failure, uncaught.get(5, TimeUnit.SECONDS));
          assertTrue(next.get(5, TimeUnit.SECONDS));
          assertFalse(caller.isDone());
          stage.complete("Ada");
          assertEquals("Ada", caller.get(5, TimeUnit.SECONDS));
        } finally {
          releaseFailure.countDown();
          stage.complete("Ada");
        }
      }
    }

    @Test
    void failedHelpedLaunchUnwindsWaitAndLetsQueuedCompletionRun() throws Exception {
      final var stage = new CompletableFuture<String>();
      final var user = Blockless.supplier(stage);
      final var enterGet = new CountDownLatch(1);
      final var launchAttempted = new CountDownLatch(1);
      final var launches = new AtomicInteger();
      final var failure = new RejectedExecutionException("helped launch failed");
      try (final var executor =
          new LazyHelpingExecutor(
              1,
              Executors.defaultThreadFactory(),
              1,
              thread -> {
                if (launches.incrementAndGet() == 1) {
                  launchAttempted.countDown();
                  throw failure;
                }
                thread.start();
              })) {
        try {
          final var caller =
              executor.submit(
                  () -> {
                    enterGet.await();
                    return user.get();
                  });
          // Only queued work can complete stage; the test thread never completes it on success.
          final var queued =
              executor.submit(
                  () -> {
                    assertFalse(Thread.currentThread().isVirtual());
                    stage.complete("Ada");
                    return "completed";
                  });
          executor.shutdown();
          enterGet.countDown();
          assertTrue(launchAttempted.await(5, TimeUnit.SECONDS));
          assertSame(
              failure,
              assertThrows(ExecutionException.class, () -> caller.get(5, TimeUnit.SECONDS))
                  .getCause());
          assertEquals("completed", queued.get(5, TimeUnit.SECONDS));
          assertEquals("Ada", stage.get(5, TimeUnit.SECONDS));
          assertEquals("Ada", user.get());
          assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
          assertEquals(1, launches.get());
        } finally {
          enterGet.countDown();
          stage.complete("Ada");
        }
      }
    }

    @Test
    void sharedOutcomeWakesWaitingWorkersInDifferentExecutors() throws Exception {
      final var stage = new CompletableFuture<String>();
      final var user = Blockless.supplier(stage);
      final var atGet = new CountDownLatch(2);
      try (final var first = LazyHelpingExecutor.newFixedThreadPool(1);
          final var second = LazyHelpingExecutor.newFixedThreadPool(1)) {
        try {
          final var one =
              first.submit(
                  () -> {
                    atGet.countDown();
                    return user.get();
                  });
          final var two =
              second.submit(
                  () -> {
                    atGet.countDown();
                    return user.get();
                  });
          assertTrue(atGet.await(5, TimeUnit.SECONDS));
          assertTrue(
              first.submit(() -> Thread.currentThread().isVirtual()).get(5, TimeUnit.SECONDS));
          assertTrue(
              second.submit(() -> Thread.currentThread().isVirtual()).get(5, TimeUnit.SECONDS));
          stage.complete("Ada");
          assertEquals("Ada", one.get(5, TimeUnit.SECONDS));
          assertEquals("Ada", two.get(5, TimeUnit.SECONDS));
        } finally {
          stage.complete("Ada");
        }
      }
    }

    @Test
    void gracefulShutdownStillHelpsPreviouslyAcceptedWork() throws Exception {
      final var started = new CountDownLatch(1);
      final var enterGet = new CountDownLatch(1);
      final var releaseBody = new CountDownLatch(1);
      try (final var executor = LazyHelpingExecutor.newFixedThreadPool(1)) {
        try {
          final var caller =
              executor.submit(
                  () -> {
                    started.countDown();
                    enterGet.await();
                    return Blockless.get(
                        () -> {
                          releaseBody.await();
                          return "Ada";
                        });
                  });
          assertTrue(started.await(5, TimeUnit.SECONDS));
          final var queued =
              executor.submit(
                  () -> {
                    releaseBody.countDown();
                    return Thread.currentThread().isVirtual();
                  });
          executor.shutdown();
          enterGet.countDown();
          assertTrue(queued.get(5, TimeUnit.SECONDS));
          assertEquals("Ada", caller.get(5, TimeUnit.SECONDS));
          assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        } finally {
          enterGet.countDown();
          releaseBody.countDown();
        }
      }
    }

    @Test
    void shutdownNowInterruptsBeforeCallingQueuedHooksAndSettlesEveryFuture() throws Exception {
      final var started = new CountDownLatch(1);
      final var hold = new CountDownLatch(1);
      final var nativeCleanup = new CountDownLatch(1);
      final var originalFailure = new IllegalStateException("foreign done failed");
      final var secondCleanup = new AtomicInteger();
      try (final var executor = LazyHelpingExecutor.newFixedThreadPool(1)) {
        try {
          final var nativeTask =
              executor.submit(
                  () -> {
                    started.countDown();
                    try {
                      hold.await();
                    } finally {
                      if (executor.isShutdown()) nativeCleanup.countDown();
                    }
                    return null;
                  });
          assertTrue(started.await(5, TimeUnit.SECONDS));
          final var first =
              new FutureTask<Void>(() -> null) {
                @Override
                protected void done() {
                  // This calls back into owner state from the interrupted native finally.
                  try {
                    assertTrue(nativeCleanup.await(1, TimeUnit.SECONDS));
                  } catch (InterruptedException failure) {
                    throw new AssertionError(failure);
                  }
                  throw originalFailure;
                }
              };
          final var second =
              new FutureTask<Void>(() -> null) {
                @Override
                protected void done() {
                  secondCleanup.incrementAndGet();
                }
              };
          executor.execute(first);
          executor.execute(second);
          assertSame(
              originalFailure, assertThrows(IllegalStateException.class, executor::shutdownNow));
          assertTrue(first.isCancelled());
          assertTrue(second.isCancelled());
          assertEquals(1, secondCleanup.get());
          assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
          assertTrue(nativeTask.isDone());
        } finally {
          hold.countDown();
        }
      }
    }

    @ParameterizedTest
    @CsvSource({"false, false", "true, false", "false, true", "true, true"})
    void interruptionAndTerminationRespectBothActualFinallyBlocks(
        boolean nativeFirst, boolean stopNow) throws Exception {
      final var atGet = new CountDownLatch(1);
      final var releaseBody = new CountDownLatch(1);
      final var foreignStarted = new CountDownLatch(1);
      final var holdForeign = new CountDownLatch(1);
      final var nativeFinally = new CountDownLatch(1);
      final var foreignFinally = new CountDownLatch(1);
      final var releaseNativeCleanup = new CountDownLatch(1);
      final var releaseForeignCleanup = new CountDownLatch(1);
      final var nativeInterrupted = new AtomicBoolean();
      final var foreignInterrupted = new AtomicBoolean();
      final var cleanups = new AtomicInteger();
      try (final var executor = LazyHelpingExecutor.newFixedThreadPool(1)) {
        try {
          final var caller =
              executor.submit(
                  () -> {
                    try {
                      final var user =
                          Blockless.supplier(
                              () -> {
                                releaseBody.await();
                                return "Ada";
                              });
                      atGet.countDown();
                      return user.get();
                    } finally {
                      nativeInterrupted.set(Thread.currentThread().isInterrupted());
                      nativeFinally.countDown();
                      awaitUninterruptibly(releaseNativeCleanup);
                      cleanups.incrementAndGet();
                    }
                  });
          assertTrue(atGet.await(5, TimeUnit.SECONDS));
          final var foreign =
              new FutureTask<Void>(
                  () -> {
                    try {
                      foreignStarted.countDown();
                      holdForeign.await();
                      return null;
                    } catch (InterruptedException failure) {
                      foreignInterrupted.set(true);
                      throw failure;
                    } finally {
                      foreignFinally.countDown();
                      awaitUninterruptibly(releaseForeignCleanup);
                      cleanups.incrementAndGet();
                    }
                  });
          // The owner sees a Runnable wrapping someone else's FutureTask.
          executor.execute(foreign::run);
          assertTrue(foreignStarted.await(5, TimeUnit.SECONDS));
          if (stopNow) executor.shutdownNow();
          else assertTrue(caller.cancel(true));
          assertTrue(nativeFinally.await(5, TimeUnit.SECONDS));
          assertTrue(nativeInterrupted.get());
          if (!stopNow) {
            assertFalse(foreign.isDone());
            assertFalse(foreignInterrupted.get());
            executor.shutdown();
            assertTrue(foreign.cancel(true));
          }
          assertThrows(RejectedExecutionException.class, () -> executor.execute(() -> {}));
          assertTrue(foreignFinally.await(5, TimeUnit.SECONDS));
          assertTrue(foreignInterrupted.get());
          assertFalse(executor.awaitTermination(20, TimeUnit.MILLISECONDS));
          (nativeFirst ? releaseNativeCleanup : releaseForeignCleanup).countDown();
          assertFalse(executor.awaitTermination(20, TimeUnit.MILLISECONDS));
          releaseNativeCleanup.countDown();
          releaseForeignCleanup.countDown();
          assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
          assertEquals(2, cleanups.get());
        } finally {
          releaseBody.countDown();
          holdForeign.countDown();
          releaseNativeCleanup.countDown();
          releaseForeignCleanup.countDown();
        }
      }
    }
  }

  private static void await(CountDownLatch latch) {
    try {
      latch.await();
    } catch (InterruptedException failure) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(failure);
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
