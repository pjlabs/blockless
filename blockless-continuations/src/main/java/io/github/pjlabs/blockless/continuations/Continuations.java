package io.github.pjlabs.blockless.continuations;

import io.github.pjlabs.blockless.AsyncSupplier;
import io.github.pjlabs.blockless.Blockless;
import io.github.pjlabs.blockless.CallableContext;
import io.github.pjlabs.blockless.ContextPropagator;
import io.github.pjlabs.blockless.SupplierWaitSupport;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
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
   * pending reads on ordinary platform threads outside a continuation throw IllegalStateException.
   * Task failures are thrown at get().
   *
   * <p>Initial execution and subsequent resumes use {@code executor}; use a queuing executor to
   * release the submitting thread. Context is captured at submission and refreshed at each
   * suspension. Each resume attaches that state and restores the executor thread's previous context
   * afterward. Tasks without a value can return {@code null}.
   *
   * <p>If a resume is rejected, the rejection is thrown at the suspended read on a fallback virtual
   * thread. The result becomes readable after the task exits, including its finally blocks.
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
    return new ContinuationResult<>((AsyncSupplier<T>) Blockless.supplier(runner.completion));
  }

  private record ContinuationResult<T>(AsyncSupplier<T> result) implements AsyncSupplier<T> {
    @Override
    public T get() {
      if (!result.isDone()
          && !Thread.currentThread().isVirtual()
          && Continuation.getCurrentContinuation(Runner.SCOPE) == null) {
        throw new IllegalStateException(
            "Pending continuation results require a continuation or virtual-thread caller");
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
    private final List<ContextPropagator> propagators;
    private Callable<Boolean> captured;
    // Confined to the serialized drain; a suspended continuation is resumed exactly once.
    private final Continuation continuation;
    private AsyncSupplier<?> pending;
    private RejectedExecutionException rejection;
    private T value;

    private Runner(Executor executor, Supplier<T> task, ContextPropagator[] propagators) {
      this.executor = executor;
      this.propagators = List.of(propagators);
      continuation = new Continuation(SCOPE, () -> value = task.get());
      captured = captureContext();
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

    private Callable<Boolean> captureContext() {
      return CallableContext.wrap(
          () -> SupplierWaitSupport.withHandler(this::suspend, this::advance), propagators);
    }

    private void signal() {
      if (signals.getAndIncrement() != 0) {
        return;
      }
      try {
        executor.execute(drain);
      } catch (Throwable failure) {
        if (pending == null) {
          // Initial submission failed: the task has not acquired any resources yet.
          completion.completeExceptionally(failure);
        } else {
          rejection =
              failure instanceof RejectedExecutionException rejected
                  ? rejected
                  : new RejectedExecutionException("Cannot resume continuation", failure);
          // Throw at the suspended read so Java unwinds the stack before publishing the outcome.
          // A virtual thread also lets finally blocks wait for asynchronous cleanup.
          Thread.ofVirtual().inheritInheritableThreadLocals(false).start(drain);
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
      // Capture while the task is still running: changes survive resumption, and capture failures
      // unwind through the task's own catch/finally blocks.
      captured = captureContext();
      pending = supplier;
      if (!Continuation.yield(SCOPE)) {
        throw new IllegalStateException("Native continuation could not suspend");
      }
      if (rejection != null) {
        throw rejection;
      }
    }
  }
}
