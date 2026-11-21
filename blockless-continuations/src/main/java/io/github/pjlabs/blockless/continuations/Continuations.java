package io.github.pjlabs.blockless.continuations;

import io.github.pjlabs.blockless.AsyncSupplier;
import io.github.pjlabs.blockless.Blockless;
import io.github.pjlabs.blockless.CallableContext;
import io.github.pjlabs.blockless.ContextPropagator;
import java.util.ArrayDeque;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import jdk.internal.vm.Continuation;
import jdk.internal.vm.ContinuationScope;

/**
 * Experimental virtual-thread suppliers with a native continuation for the executor-side wait.
 * Requires Java 21+ with java.base/jdk.internal.vm exported to this module (or ALL-UNNAMED on the
 * classpath).
 */
public final class Continuations {
  private static final ThreadLocal<ArrayDeque<Runnable>> DRAINS = new ThreadLocal<>();

  private Continuations() {}

  private static void runDrains(Runnable task) {
    final var active = DRAINS.get();
    if (active != null) {
      active.addLast(task);
      return;
    }
    final var queue = new ArrayDeque<Runnable>();
    DRAINS.set(queue);
    try {
      queue.addLast(task);
      for (var next = queue.pollFirst(); next != null; next = queue.pollFirst()) {
        next.run();
      }
    } finally {
      DRAINS.remove();
    }
  }

  /**
   * Starts a virtual-thread supplier using the shared common pool for dispatch and completion.
   * Context propagators are optional. See {@link #supplier(Executor, Callable,
   * ContextPropagator...)} for result-read and failure behavior.
   */
  public static <T> AsyncSupplier<T> supplier(Callable<T> task, ContextPropagator... propagators) {
    return supplier(ForkJoinPool.commonPool(), task, propagators);
  }

  /**
   * Dispatches work onto a virtual thread and returns a supplier of its cached outcome. A native
   * continuation releases the executor worker while the body runs, including ordinary blocking I/O.
   * Virtual-thread callers park on pending reads. Pending reads on platform threads throw
   * IllegalStateException. Task failures are thrown at get().
   *
   * <p>Use a queuing executor for dispatch and completion. The body always runs on a virtual
   * thread; configured context is captured at submission and attached for the body's lifetime.
   * Changes survive blocking calls. Executor workers have their previous context restored
   * afterward. Tasks without a value can return {@code null}.
   *
   * <p>An initial rejection prevents the body from starting. If a completion resume is rejected, a
   * fallback virtual thread publishes the body's outcome. The body is never restarted and its
   * finally blocks finish before the outcome becomes readable.
   */
  public static <T> AsyncSupplier<T> supplier(
      Executor executor, Callable<T> task, ContextPropagator... propagators) {
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
    return new ContinuationResult<>((AsyncSupplier<T>) Blockless.supplier(runner.completion));
  }

  private record ContinuationResult<T>(AsyncSupplier<T> result) implements AsyncSupplier<T> {
    @Override
    public T get() {
      if (!result.isDone() && !Thread.currentThread().isVirtual()) {
        throw new IllegalStateException(
            "Pending continuation results require a virtual-thread caller");
      }
      return result.get();
    }

    @Override
    public boolean isDone() {
      return result.isDone();
    }

    @Override
    public void onComplete(Runnable action) {
      result.onComplete(action);
    }
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

    private Runner(Executor executor, Callable<T> task, ContextPropagator[] propagators) {
      this.executor = executor;
      final var work = CallableContext.wrap(task, propagators);
      continuation =
          new Continuation(
              SCOPE,
              () -> {
                final var result = (AsyncSupplier<T>) Blockless.supplier(work);
                if (!result.isDone()) {
                  suspend(result);
                }
                value = result.get();
              });
      final var captured = CallableContext.wrap(this::advance, propagators);
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
        executor.execute(() -> runDrains(drain));
      } catch (Throwable failure) {
        if (pending == null) {
          // Initial submission failed: the task has not acquired any resources yet.
          completion.completeExceptionally(failure);
        } else {
          // The body and its finally blocks have already finished. Publish its actual outcome,
          // even if its executor has closed; never restart work or replace a successful result.
          Thread.ofVirtual().inheritInheritableThreadLocals(false).start(() -> runDrains(drain));
        }
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
