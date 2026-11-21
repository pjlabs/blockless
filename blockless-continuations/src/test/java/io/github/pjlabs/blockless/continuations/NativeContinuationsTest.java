package io.github.pjlabs.blockless.continuations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.pjlabs.blockless.Blockless;
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
class NativeContinuationsTest {
  @Test
  void aLambdaAndUnannotatedHelpersResumeOnADifferentPlatformWorker() throws Exception {
    final var stage = new CompletableFuture<String>();
    final var before = new AtomicReference<Thread>();
    final var after = new AtomicReference<Thread>();
    final var starts = new AtomicInteger();
    final var submissions = new AtomicInteger();
    final var result = new AtomicReference<String>();
    try (final var first = Executors.newSingleThreadExecutor();
        final var second = Executors.newSingleThreadExecutor()) {
      final var completion =
          Continuations.run(
              task -> (submissions.getAndIncrement() == 0 ? first : second).execute(task),
              () -> {
                before.set(Thread.currentThread());
                starts.incrementAndGet();
                result.set(helper(stage, 42L));
                after.set(Thread.currentThread());
              });
      first.submit(() -> {}).get(5, TimeUnit.SECONDS);
      assertFalse(completion.toCompletableFuture().isDone());
      stage.complete("ready");
      completion.toCompletableFuture().get(5, TimeUnit.SECONDS);
      assertEquals("saved42ready", result.get());
      assertEquals(1, starts.get());
      assertFalse(before.get().isVirtual());
      assertFalse(after.get().isVirtual());
      assertNotSame(before.get(), after.get());
    }
  }

  @Test
  void missingJvmExportFailsBeforeTheTaskRuns() throws Exception {
    final var java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
    final var process =
        new ProcessBuilder(
                java,
                "-cp",
                System.getProperty("java.class.path"),
                NativeLaunchProbe.class.getName())
            .redirectErrorStream(true)
            .start();
    try {
      assertTrue(process.waitFor(10, TimeUnit.SECONDS));
      final var output =
          new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      assertEquals(0, process.exitValue(), output);
      assertTrue(output.contains("missing export rejected before execution"), output);
    } finally {
      process.destroyForcibly();
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

  private static String helper(CompletableFuture<String> stage, long number) {
    final var prefix = "saved" + number;
    return prefix + inner(stage);
  }

  private static String inner(CompletableFuture<String> stage) {
    return Blockless.get(stage);
  }
}
