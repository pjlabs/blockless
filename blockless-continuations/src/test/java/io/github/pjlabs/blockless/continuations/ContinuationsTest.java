package io.github.pjlabs.blockless.continuations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.github.pjlabs.blockless.Blockless;
import io.github.pjlabs.blockless.ContextPropagator;
import io.github.pjlabs.blockless.Parallel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(15)
class ContinuationsTest {
  @Test
  void suppliersStartEagerlyAndReturnValuesAcrossSuspendedCallers() throws Exception {
    final var stage = new CompletableFuture<String>();
    final var entries = new AtomicInteger();
    final var result = new AtomicReference<String>();
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
      final var completion = Continuations.run(executor, () -> result.set(combined.get()));
      // The only platform worker remains available while both suppliers and their caller suspend.
      executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
      assertFalse(completion.toCompletableFuture().isDone());
      stage.complete("ready");
      completion.toCompletableFuture().get(5, TimeUnit.SECONDS);
      assertEquals("[saved:ready]", result.get());
      assertEquals("saved:ready", value.get());
      assertEquals(1, entries.get());
    } finally {
      stage.complete("release waiter");
    }
  }

  @Test
  void suspendsNestedFramesAndRestoresContextOnAnotherWorker() throws Exception {
    final var stage = new CompletableFuture<String>();
    final var result = new AtomicReference<String>();
    final var entries = new AtomicInteger();
    final var cleanups = new AtomicInteger();
    final var submissions = new AtomicInteger();
    final var context = new ThreadLocal<String>();
    context.set("request");
    try (final var first = Executors.newSingleThreadExecutor();
        final var second =
            Executors.newSingleThreadExecutor(Thread.ofPlatform().name("resume").factory())) {
      first.submit(() -> context.set("executor")).get(5, TimeUnit.SECONDS);
      second.submit(() -> context.set("executor")).get(5, TimeUnit.SECONDS);
      final var completion =
          Continuations.run(
              task -> (submissions.getAndIncrement() == 0 ? first : second).execute(task),
              () -> {
                entries.incrementAndGet();
                try {
                  assertEquals("request", context.get());
                  result.set(nested(stage, 42L));
                  assertEquals("request", context.get());
                  assertEquals("resume", Thread.currentThread().getName());
                  assertFalse(Thread.currentThread().isVirtual());
                } finally {
                  cleanups.incrementAndGet();
                }
              },
              new ThreadContext(context));
      assertEquals("executor", first.submit(context::get).get(5, TimeUnit.SECONDS));
      assertFalse(completion.toCompletableFuture().isDone());
      assertEquals(0, cleanups.get());
      stage.complete("ready");
      completion.toCompletableFuture().get(5, TimeUnit.SECONDS);
      assertEquals("saved42ready", result.get());
      assertEquals(1, entries.get());
      assertEquals(1, cleanups.get());
      assertEquals("executor", second.submit(context::get).get(5, TimeUnit.SECONDS));
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
      final var completion = Continuations.run(executor, value::get);
      executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
      stage.complete("ready");
      assertSame(
          failure,
          assertThrows(
                  ExecutionException.class,
                  () -> completion.toCompletableFuture().get(5, TimeUnit.SECONDS))
              .getCause());
    }
  }

  @Test
  void rejectedResumeCompletesTheTaskExceptionally() throws Exception {
    final var stage = new CompletableFuture<String>();
    try (final var executor = Executors.newSingleThreadExecutor()) {
      final var completion = Continuations.run(executor, () -> Blockless.get(stage));
      executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
      executor.shutdown();
      stage.complete("ready");
      assertInstanceOf(
          RejectedExecutionException.class,
          assertThrows(
                  ExecutionException.class,
                  () -> completion.toCompletableFuture().get(5, TimeUnit.SECONDS))
              .getCause());
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
            () -> Continuations.run(Runnable::run, () -> fail("task must not run")));
    assertTrue(
        failure.getMessage().contains("--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED"));
  }

  private static String nested(CompletableFuture<String> stage, long number) {
    final var prefix = "saved" + number;
    return prefix + Parallel.create().async(stage::join).get();
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
