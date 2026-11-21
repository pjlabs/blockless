package io.github.pjlabs.blockless.continuations;

import io.github.pjlabs.blockless.AsyncSupplier;
import io.github.pjlabs.blockless.Blockless;
import io.github.pjlabs.blockless.CallableContext;
import io.github.pjlabs.blockless.ContextPropagator;
import io.github.pjlabs.blockless.SupplierWaitSupport;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import jdk.internal.vm.Continuation;
import jdk.internal.vm.ContinuationScope;

/**
 * Experimental native continuations around Blockless supplier reads. Requires Java 21+ with
 * java.base/jdk.internal.vm exported to this module (or ALL-UNNAMED on the classpath).
 */
public final class Continuations {
  private Continuations() {}

  /**
   * Starts a value-producing continuation immediately and returns a supplier of its cached outcome.
   * Pending reads inside another continuation suspend that caller; virtual-thread reads park, and
   * ordinary platform-thread reads outside a continuation wait. Task failures are thrown at get().
   *
   * <p>Initial execution and subsequent resumes use {@code executor}; use a queuing executor to
   * release the submitting thread. Each resume attaches the context captured here and restores the
   * executor thread's previous context. Tasks without a value can return {@code null}.
   */
  public static <T> AsyncSupplier<T> supplier(
      Executor executor, Supplier<T> task, ContextPropagator... propagators) {
    Objects.requireNonNull(executor, "executor");
    Objects.requireNonNull(task, "task");
    final var apiModule = Continuations.class.getModule();
    if (!Object.class.getModule().isExported("jdk.internal.vm", apiModule)) {
      final var target = apiModule.isNamed() ? apiModule.getName() : "ALL-UNNAMED";
      throw new IllegalStateException(
          "Experimental native continuations require --add-exports=java.base/jdk.internal.vm="
              + target);
    }
    final var runner = new Runner<T>(executor, task, propagators);
    runner.signal();
    return (AsyncSupplier<T>) Blockless.supplier(runner.completion);
  }

  private static final class Runner<T> {
    // Initialize native types only after supplier() has checked the required JVM export.
    private static final ContinuationScope SCOPE = new ContinuationScope("blockless");
    private final Executor executor;
    private final CompletableFuture<T> completion = new CompletableFuture<>();
    private final AtomicInteger signals = new AtomicInteger();
    private final Runnable drain;
    // Confined to the serialized drain; a suspended continuation is resumed exactly once.
    private final Continuation continuation;
    private AsyncSupplier<?> pending;
    private T value;

    private Runner(Executor executor, Supplier<T> task, ContextPropagator[] propagators) {
      this.executor = executor;
      continuation = new Continuation(SCOPE, () -> value = task.get());
      final var captured =
          CallableContext.wrap(
              () -> SupplierWaitSupport.withHandler(this::suspend, this::advance), propagators);
      drain =
          () -> {
            try {
              if (captured.call()) {
                completion.complete(value);
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
        pending = null;
        continuation.run();
        if (continuation.isDone()) {
          return true;
        }
        if (pending == null) {
          throw new IllegalStateException("Unexpected continuation suspension");
        }
        // Register only after the complete stack has unwound. Inline notification increments
        // signals instead of recursively resuming, including with a direct executor.
        pending.onComplete(this::signal);
      } while (signals.decrementAndGet() != 0);
      return false;
    }

    private void suspend(AsyncSupplier<?> supplier) {
      pending = supplier;
      if (!Continuation.yield(SCOPE)) {
        throw new IllegalStateException("Native continuation could not suspend");
      }
    }
  }
}
