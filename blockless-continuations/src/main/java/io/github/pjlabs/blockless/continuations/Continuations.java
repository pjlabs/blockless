package io.github.pjlabs.blockless.continuations;

import io.github.pjlabs.blockless.AsyncSupplier;
import io.github.pjlabs.blockless.Blockless;
import io.github.pjlabs.blockless.CallableContext;
import io.github.pjlabs.blockless.ContextPropagator;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.commons.javaflow.api.Continuation;
import org.apache.commons.javaflow.core.Skip;

/** Schedules bytecode continuations around Blockless supplier reads. */
public final class Continuations {
  private static final ThreadLocal<Boolean> ACTIVE = new ThreadLocal<>();

  private Continuations() {}

  /**
   * Submits a task and returns without waiting for its results. Initial execution and subsequent
   * resumes use {@code executor}; use a queuing executor to release the submitting thread. Each
   * resume attaches the context captured here and restores the executor thread's previous context.
   * The returned stage reports task completion or failure; it does not expose cancellation.
   */
  public static CompletionStage<Void> run(
      Executor executor, ContinuationTask task, ContextPropagator... propagators) {
    Objects.requireNonNull(executor, "executor");
    Objects.requireNonNull(task, "task");
    if (!task.getClass().isAnnotationPresent(Skip.class)) {
      throw new IllegalArgumentException(
          "ContinuationTask must be enhanced by ContinuationEnhancer");
    }
    final var runner = new Runner(executor, task, propagators);
    runner.signal();
    return runner.completion.minimalCompletionStage();
  }

  /** Bytecode target for Supplier.get(). Application code uses the ordinary supplier API. */
  @Suspendable
  public static <T> T await(Supplier<T> supplier) {
    Objects.requireNonNull(supplier, "supplier");
    if (!Boolean.TRUE.equals(ACTIVE.get()) || Thread.currentThread().isVirtual()) {
      return supplier.get();
    }
    if (!(supplier instanceof AsyncSupplier<?> asynchronous)) {
      throw new IllegalArgumentException("Only Blockless.supplier and Parallel.async can suspend");
    }
    if (!asynchronous.isDone()) {
      Continuation.suspend(asynchronous);
    }
    return supplier.get();
  }

  /** Bytecode target for Blockless.get(Callable). */
  @Suspendable
  public static <T> T get(Callable<T> callable) {
    return await(Blockless.supplier(callable));
  }

  /** Bytecode target for Blockless.get(CompletionStage). */
  @Suspendable
  public static <T> T get(CompletionStage<T> stage) {
    return await(Blockless.supplier(stage));
  }

  private static final class Runner {
    private final Executor executor;
    private final CompletableFuture<Void> completion = new CompletableFuture<>();
    private final AtomicInteger signals = new AtomicInteger();
    private final Runnable drain;
    // Confined to the serialized drain; a suspended continuation is resumed exactly once.
    private Continuation continuation;

    private Runner(Executor executor, ContinuationTask task, ContextPropagator[] propagators) {
      this.executor = executor;
      continuation = Continuation.startSuspendedWith(task, true);
      final var captured = CallableContext.wrap(this::advance, propagators);
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
      final var previous = ACTIVE.get();
      ACTIVE.set(true);
      try {
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
      } finally {
        if (previous == null) {
          ACTIVE.remove();
        } else {
          ACTIVE.set(previous);
        }
      }
    }
  }
}
