package io.github.pjlabs.blockless.continuations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.github.pjlabs.blockless.AsyncSupplier;
import io.github.pjlabs.blockless.Blockless;
import io.github.pjlabs.blockless.ContextPropagator;
import io.github.pjlabs.blockless.Parallel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
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
  void pendingReadCannotDeadlockTheOnlyWorkerNeededToRunTheTask() throws Exception {
    try (final var executor = Executors.newSingleThreadExecutor()) {
      final var caller =
          executor.submit(
              () -> {
                final var result = Continuations.supplier(executor, () -> 42);
                assertThrows(IllegalStateException.class, result::get);
                return result;
              });
      try {
        final var result = caller.get(5, TimeUnit.SECONDS);
        assertEquals(42, await(result));
        assertEquals(42, result.get());
      } finally {
        caller.cancel(true);
      }
    }
  }

  @Test
  void suppliersStartEagerlyAndReturnValuesAcrossSuspendedCallers() throws Exception {
    final var stage = new CompletableFuture<String>();
    final var entries = new AtomicInteger();
    try (final var executor = Executors.newSingleThreadExecutor()) {
      final var value =
          Continuations.supplier(
              executor,
              () -> {
                entries.incrementAndGet();
                return "saved:" + Blockless.get(stage);
              });
      final var combined = Continuations.supplier(executor, () -> "[" + value.get() + "]");
      executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
      assertEquals(1, entries.get());
      final var completion = Continuations.supplier(executor, combined::get);
      // The only platform worker remains available while both suppliers and their caller suspend.
      executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
      assertFalse(completion.isDone());
      stage.complete("ready");
      assertEquals("[saved:ready]", await(completion));
      assertEquals("saved:ready", value.get());
      assertEquals(1, entries.get());
    } finally {
      stage.complete("release waiter");
    }
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = "changed inside flow")
  void preservesContextChangesAndRestoresBothWorkers(String changed) throws Exception {
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
                  assertEquals("request", context.get());
                  if (changed == null) context.remove();
                  else context.set(changed);
                  final var result = nested(stage, 42L);
                  assertEquals(changed, context.get());
                  assertEquals("resume", Thread.currentThread().getName());
                  assertFalse(Thread.currentThread().isVirtual());
                  return result;
                } finally {
                  cleanups.incrementAndGet();
                }
              },
              new ThreadContext(context));
      assertEquals("first worker", first.submit(context::get).get(5, TimeUnit.SECONDS));
      assertFalse(completion.isDone());
      assertEquals(0, cleanups.get());
      stage.complete("ready");
      assertEquals("saved42ready", await(completion));
      assertEquals(1, entries.get());
      assertEquals(1, cleanups.get());
      assertEquals("second worker", second.submit(context::get).get(5, TimeUnit.SECONDS));
      assertEquals("request", context.get());
    } finally {
      stage.complete("release waiter");
      context.remove();
    }
  }

  @Test
  void reportsTaskFailureAfterResumption() throws Exception {
    final var stage = new CompletableFuture<String>();
    final var failure = new IllegalStateException("resumed task failed");
    try (final var executor = Executors.newSingleThreadExecutor()) {
      final var value =
          Continuations.supplier(
              executor,
              () -> {
                Blockless.get(stage);
                throw failure;
              });
      final var completion = Continuations.supplier(executor, value::get);
      executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
      stage.complete("ready");
      assertSame(failure, assertThrows(IllegalStateException.class, () -> await(completion)));
    }
  }

  @Test
  void rejectedResumeUnwindsFinallyBeforeCompletingTheResult() throws Exception {
    final var stage = new CompletableFuture<String>();
    final var cleanup = new CompletableFuture<Void>();
    final var cleanupStarted = new CountDownLatch(1);
    final var cleanups = new AtomicInteger();
    final var continued = new AtomicBoolean();
    final var cleanupIsVirtual = new AtomicBoolean();
    final var context = new ThreadLocal<String>();
    context.set("request");
    try (final var executor = Executors.newSingleThreadExecutor()) {
      final var completion =
          Continuations.supplier(
              executor,
              () -> {
                context.set("changed before rejection");
                try {
                  Blockless.get(stage);
                  continued.set(true);
                  return "unexpected success";
                } finally {
                  assertEquals("changed before rejection", context.get());
                  cleanupIsVirtual.set(Thread.currentThread().isVirtual());
                  cleanupStarted.countDown();
                  Blockless.get(cleanup);
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
      assertThrows(RejectedExecutionException.class, () -> await(completion));
      assertEquals(1, cleanups.get());
      assertFalse(continued.get());
      assertEquals("request", context.get());

      // An initial rejection must not start the task on the cleanup thread.
      final var rejected = Continuations.supplier(executor, () -> fail("task must not start"));
      assertThrows(RejectedExecutionException.class, rejected::get);
    } finally {
      stage.complete("release waiter");
      cleanup.complete(null);
      context.remove();
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

  // Entry point for the child JVM, which intentionally has no export flag.
  public static void main(String[] args) {
    final var failure =
        assertThrows(
            IllegalStateException.class,
            () -> Continuations.supplier(Runnable::run, () -> fail("task must not run")));
    assertTrue(
        failure.getMessage().contains("--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED"));
  }

  private static String nested(CompletableFuture<String> stage, long number) {
    final var prefix = "saved" + number;
    return prefix + Parallel.create().async(stage::join).get();
  }

  private static <T> T await(AsyncSupplier<T> supplier) throws Exception {
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
