package io.github.pjlabs.blockless.continuations;

import io.github.pjlabs.blockless.AsyncSupplier;
import io.github.pjlabs.blockless.CallableContext;
import io.github.pjlabs.blockless.ContextPropagator;
import io.github.pjlabs.blockless.SupplierWaitSupport;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.javaflow.api.Continuation;

/**
 * Experimental native continuations around Blockless supplier reads. Requires Java 21+ with
 * java.base/jdk.internal.vm exported to this module (or ALL-UNNAMED on the classpath).
 */
public final class Continuations {
  private Continuations() {}

  /**
   * Submits a task and returns without waiting for its results. Initial execution and subsequent
   * resumes use {@code executor}; use a queuing executor to release the submitting thread. Each
   * resume attaches the context captured here and restores the executor thread's previous context.
   * The returned stage reports task completion or failure; it does not expose cancellation.
   */
  public static CompletionStage<Void> run(
      Executor executor, Runnable task, ContextPropagator... propagators) {
    Objects.requireNonNull(executor, "executor");
    Objects.requireNonNull(task, "task");
    final var apiModule = Continuation.class.getModule();
    if (!Object.class.getModule().isExported("jdk.internal.vm", apiModule)) {
      final var target = apiModule.isNamed() ? apiModule.getName() : "ALL-UNNAMED";
      throw new IllegalStateException(
          "Experimental native continuations require --add-exports=java.base/jdk.internal.vm="
              + target);
    }
    final var runner = new Runner(executor, task, propagators);
    runner.signal();
    return runner.completion.minimalCompletionStage();
  }

  private static final class Runner {
    private final Executor executor;
    private final CompletableFuture<Void> completion = new CompletableFuture<>();
    private final AtomicInteger signals = new AtomicInteger();
    private final Runnable drain;
    // Confined to the serialized drain; a suspended continuation is resumed exactly once.
    private Continuation continuation;

    private Runner(Executor executor, Runnable task, ContextPropagator[] propagators) {
      this.executor = executor;
      continuation = Continuation.startSuspendedWith(task, true);
      final var captured =
          CallableContext.wrap(
              () -> SupplierWaitSupport.withHandler(Continuation::suspend, this::advance),
              propagators);
      drain =
          () -> {
            try {
              if (captured.call()) {
                completion.complete(null);
              }
            } catch (Throwable failure) {
              completion.completeExceptionally(failure);
            }
          };
    }

    private void signal() {
      if (signals.getAndIncrement() != 0) {
        return;
      }
      try {
        executor.execute(drain);
      } catch (Throwable failure) {
        completion.completeExceptionally(failure);
      }
    }

    private boolean advance() {
      do {
        continuation = continuation.resume();
        if (continuation == null) {
          return true;
        }
        if (!(continuation.value() instanceof AsyncSupplier<?> pending)) {
          throw new IllegalStateException("Unexpected continuation suspension");
        }
        // Register only after the complete stack has unwound. Inline notification increments
        // signals instead of recursively resuming, including with a direct executor.
        pending.onComplete(this::signal);
      } while (signals.decrementAndGet() != 0);
      return false;
    }
  }
}
