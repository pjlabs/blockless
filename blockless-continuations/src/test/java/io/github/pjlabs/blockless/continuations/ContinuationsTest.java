package io.github.pjlabs.blockless.continuations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.github.pjlabs.blockless.AsyncSupplier;
import io.github.pjlabs.blockless.ContextPropagator;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

@Timeout(15)
class ContinuationsTest {
  @Test
  void blockingCodeNeedsOnlyOneSupplier() throws Exception {
    final var greeting =
        Continuations.supplier(
            () -> {
              assertTrue(Thread.currentThread().isVirtual());
              Thread.sleep(10);
              return "hello";
            });

    assertEquals("hello", awaitResult(greeting));
    assertEquals("hello", greeting.get());
  }

  @Test
  void independentSuppliersStartInParallelBeforeTheirResultsAreRead() throws Exception {
    final var bothStarted = new CountDownLatch(2);
    final var finishQueries = new CountDownLatch(1);
    try (final var executor = Executors.newSingleThreadExecutor()) {
      final var user =
          Continuations.supplier(
              executor,
              () -> {
                final var details =
                    Continuations.supplier(
                        executor,
                        () -> {
                          bothStarted.countDown();
                          finishQueries.await();
                          return "Alice";
                        });
                final var permissions =
                    Continuations.supplier(
                        executor,
                        () -> {
                          bothStarted.countDown();
                          finishQueries.await();
                          return "admin";
                        });
                return details.get() + ": " + permissions.get();
              });

      assertTrue(
          bothStarted.await(5, TimeUnit.SECONDS), "both queries start before either finishes");
      // Even with only one platform worker, unrelated work can run while the queries wait.
      executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
      assertFalse(user.isDone());
      finishQueries.countDown();
      assertEquals("Alice: admin", awaitResult(user));
    } finally {
      finishQueries.countDown();
    }
  }

  @Test
  void dependentSuppliersPropagateTheOriginalFailure() throws Exception {
    final var stage = new CompletableFuture<String>();
    final var failure = new IllegalStateException("resumed task failed");
    try (final var executor = Executors.newSingleThreadExecutor()) {
      final var value =
          Continuations.supplier(
              executor,
              () -> {
                stage.get();
                throw failure;
              });
      final var completion = Continuations.supplier(executor, value::get);
      executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
      stage.complete("ready");
      assertSame(failure, assertThrows(IllegalStateException.class, () -> awaitResult(completion)));
    } finally {
      stage.complete("release waiter");
    }
  }

  @Test
  void pendingResultReadsFailFastOnPlatformThreads() throws Exception {
    try (final var executor = Executors.newSingleThreadExecutor()) {
      final var caller =
          executor.submit(
              () -> {
                // This worker must return before its queued supplier can start.
                final var result = Continuations.supplier(executor, () -> 42);
                assertThrows(IllegalStateException.class, result::get);
                return result;
              });
      try {
        final var result = caller.get(5, TimeUnit.SECONDS);
        assertEquals(42, awaitResult(result));
        assertEquals(42, result.get());
      } finally {
        caller.cancel(true);
      }
    }
  }

  @Test
  void initialExecutorRejectionDoesNotStartTheBody() {
    try (final var executor = Executors.newSingleThreadExecutor()) {
      executor.shutdown();
      final var result = Continuations.supplier(executor, () -> fail("body must not start"));
      assertThrows(RejectedExecutionException.class, result::get);
    }
  }

  @Test
  void missingExportFailsBeforeExecutingTheTask() throws Exception {
    final var java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    final var process =
        new ProcessBuilder(
                java,
                "-cp",
                System.getProperty("java.class.path"),
                ContinuationsTest.class.getName())
            .redirectErrorStream(true)
            .start();
    try {
      assertTrue(process.waitFor(10, TimeUnit.SECONDS));
      assertEquals(
          0,
          process.exitValue(),
          new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
    } finally {
      process.destroyForcibly();
    }
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = "changed inside flow")
  void optionalContextSurvivesBlockingAndIsRestoredOnExecutorWorkers(String changed)
      throws Exception {
    final var stage = new CompletableFuture<String>();
    final var entries = new AtomicInteger();
    final var cleanups = new AtomicInteger();
    final var submissions = new AtomicInteger();
    final var context = new ThreadLocal<String>();
    context.set("request");
    try (final var first = Executors.newSingleThreadExecutor();
        final var second =
            Executors.newSingleThreadExecutor(Thread.ofPlatform().name("resume").factory())) {
      first.submit(() -> context.set("first worker")).get(5, TimeUnit.SECONDS);
      second.submit(() -> context.set("second worker")).get(5, TimeUnit.SECONDS);
      final var completion =
          Continuations.supplier(
              task -> (submissions.getAndIncrement() == 0 ? first : second).execute(task),
              () -> {
                entries.incrementAndGet();
                try {
                  final var bodyThread = Thread.currentThread();
                  assertTrue(bodyThread.isVirtual());
                  assertEquals("request", context.get());
                  if (changed == null) context.remove();
                  else context.set(changed);
                  final var result = stage.get();
                  assertEquals(changed, context.get());
                  assertSame(bodyThread, Thread.currentThread());
                  return result;
                } finally {
                  cleanups.incrementAndGet();
                }
              },
              new ThreadContext(context));
      final var completionThread = new CompletableFuture<String>();
      completion.onComplete(() -> completionThread.complete(Thread.currentThread().getName()));
      assertEquals("first worker", first.submit(context::get).get(5, TimeUnit.SECONDS));
      assertFalse(completion.isDone());
      assertEquals(0, cleanups.get());
      stage.complete("ready");
      assertEquals("ready", awaitResult(completion));
      assertEquals("resume", completionThread.get(5, TimeUnit.SECONDS));
      assertEquals(1, entries.get());
      assertEquals(1, cleanups.get());
      assertEquals("second worker", second.submit(context::get).get(5, TimeUnit.SECONDS));
      assertEquals("request", context.get());
    } finally {
      stage.complete("release waiter");
      context.remove();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void executorShutdownPreservesBodyOutcomeAndWaitsForCleanup(boolean fails) throws Exception {
    final var stage = new CompletableFuture<String>();
    final var cleanup = new CompletableFuture<Void>();
    final var cleanupStarted = new CountDownLatch(1);
    final var cleanups = new AtomicInteger();
    final var cleanupIsVirtual = new AtomicBoolean();
    final var context = new ThreadLocal<String>();
    final var failure = new IllegalStateException("body failed");
    context.set("request");
    try (final var executor = Executors.newSingleThreadExecutor()) {
      final var completion =
          Continuations.supplier(
              executor,
              () -> {
                context.set("changed before rejection");
                try {
                  final var value = stage.get();
                  if (fails) throw failure;
                  return value;
                } finally {
                  assertEquals("changed before rejection", context.get());
                  cleanupIsVirtual.set(Thread.currentThread().isVirtual());
                  cleanupStarted.countDown();
                  cleanup.get();
                  cleanups.incrementAndGet();
                }
              },
              new ThreadContext(context));
      executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
      executor.shutdown();
      stage.complete("ready");
      assertTrue(cleanupStarted.await(5, TimeUnit.SECONDS));
      assertTrue(cleanupIsVirtual.get());
      assertFalse(completion.isDone(), "cleanup must finish before the result is published");
      cleanup.complete(null);
      if (fails)
        assertSame(
            failure, assertThrows(IllegalStateException.class, () -> awaitResult(completion)));
      else assertEquals("ready", awaitResult(completion));
      assertEquals(1, cleanups.get());
      assertEquals("request", context.get());
    } finally {
      stage.complete("release waiter");
      cleanup.complete(null);
      context.remove();
    }
  }

  @Test
  void nestedSuppliersStartInParallelFromAVirtualCallerWithADirectExecutor() throws Exception {
    final var started = new CountDownLatch(2);
    final var release = new CountDownLatch(1);
    final Callable<Integer> body =
        () -> {
          started.countDown();
          release.await();
          return 1;
        };
    try (final var callers = Executors.newVirtualThreadPerTaskExecutor()) {
      final var caller =
          callers.submit(
              () -> {
                final var first = Continuations.supplier(Runnable::run, body);
                final var second = Continuations.supplier(Runnable::run, body);
                return first.get() + second.get();
              });
      try {
        assertTrue(
            started.await(5, TimeUnit.SECONDS), "both bodies must start before either finishes");
        release.countDown();
        assertEquals(2, caller.get(5, TimeUnit.SECONDS));
      } finally {
        release.countDown();
        caller.cancel(true);
      }
    }
  }

  @Test
  void inlineCompletionCallbacksDoNotRecursivelyDrainOtherRunners() throws Exception {
    final var finishBodies = new CountDownLatch(1);
    final var executorQueues = new ArrayList<LinkedBlockingQueue<Runnable>>();
    final var suppliers = new ArrayList<AsyncSupplier<Integer>>();
    final var callbackDepth = new AtomicInteger();
    final var maximumCallbackDepth = new AtomicInteger();
    final var completedCallbacks = new AtomicInteger();
    try {
      // Start every body, but hold its completion task in a separate executor queue.
      for (int i = 0; i < 64; i++) {
        final var value = i;
        final var queue = new LinkedBlockingQueue<Runnable>();
        executorQueues.add(queue);
        suppliers.add(
            Continuations.supplier(
                queue::add,
                () -> {
                  finishBodies.await();
                  return value;
                }));
        final var start = queue.poll(5, TimeUnit.SECONDS);
        assertNotNull(start);
        start.run();
      }
      finishBodies.countDown();
      final var completionTasks = new ArrayList<Runnable>();
      for (final var queue : executorQueues) {
        final var resume = queue.poll(5, TimeUnit.SECONDS);
        assertNotNull(resume);
        completionTasks.add(resume);
      }
      // Each callback runs the next supplier's completion inline on this same thread.
      for (int i = 0; i < suppliers.size(); i++) {
        final var next = i + 1;
        suppliers
            .get(i)
            .onComplete(
                () -> {
                  maximumCallbackDepth.accumulateAndGet(callbackDepth.incrementAndGet(), Math::max);
                  try {
                    completedCallbacks.incrementAndGet();
                    if (next < completionTasks.size()) completionTasks.get(next).run();
                  } finally {
                    callbackDepth.decrementAndGet();
                  }
                });
      }
      completionTasks.getFirst().run();
      assertEquals(suppliers.size(), completedCallbacks.get());
      for (int i = 0; i < suppliers.size(); i++) assertEquals(i, suppliers.get(i).get());
      assertEquals(1, maximumCallbackDepth.get());
      // The thread-local dispatch queue must not retain later work after draining.
      assertEquals(42, awaitResult(Continuations.supplier(Runnable::run, () -> 42)));
    } finally {
      finishBodies.countDown();
    }
  }

  // Entry point for the child JVM, which intentionally has no export flag.
  public static void main(String[] args) {
    final var failure =
        assertThrows(
            IllegalStateException.class,
            () -> Continuations.supplier(() -> fail("task must not run")));
    assertTrue(
        failure.getMessage().contains("--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED"));
  }

  // The JUnit thread waits for readiness here; application code can call get() on a virtual thread.
  private static <T> T awaitResult(AsyncSupplier<T> supplier) throws Exception {
    final var ready = new CompletableFuture<Void>();
    supplier.onComplete(() -> ready.complete(null));
    ready.get(5, TimeUnit.SECONDS);
    return supplier.get();
  }

  private record ThreadContext(ThreadLocal<String> context) implements ContextPropagator {
    @Override
    public Object capture() {
      return context.get();
    }

    @Override
    public Object attach(Object captured) {
      final var previous = context.get();
      context.set((String) captured);
      return previous;
    }

    @Override
    public void restore(Object previous) {
      if (previous == null) context.remove();
      else context.set((String) previous);
    }
  }
}
