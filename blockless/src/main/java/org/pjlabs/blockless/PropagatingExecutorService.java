package org.pjlabs.blockless;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An {@link ExecutorService} that propagates context from the submitting thread to the executing
 * thread using the configured {@link ContextPropagator} instances.
 *
 * <p>Every task submitted through this executor is wrapped so that context is captured at
 * submission time, attached before execution, and restored after execution.
 */
public final class PropagatingExecutorService implements ExecutorService {

  private final ExecutorService delegate;
  private final List<ContextPropagator> propagators;

  private PropagatingExecutorService(
      ExecutorService delegate, List<ContextPropagator> propagators) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.propagators = List.copyOf(propagators);
  }

  /** Wraps an existing {@link ExecutorService} with context propagation. */
  public static ExecutorService wrap(ExecutorService delegate, ContextPropagator... propagators) {
    return wrap(delegate, List.of(propagators));
  }

  /** Wraps an existing {@link ExecutorService} with context propagation. */
  public static ExecutorService wrap(
      ExecutorService delegate, List<ContextPropagator> propagators) {
    return new PropagatingExecutorService(delegate, propagators);
  }

  // --- Task submission: wrap before delegating ---

  @Override
  public void execute(Runnable command) {
    delegate.execute(RunnableContext.wrap(command, propagators));
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    return delegate.submit(CallableContext.wrap(task, propagators));
  }

  @Override
  public Future<?> submit(Runnable task) {
    return delegate.submit(RunnableContext.wrap(task, propagators));
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return delegate.submit(RunnableContext.wrap(task, propagators), result);
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    return delegate.invokeAll(wrapAll(tasks));
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException {
    return delegate.invokeAll(wrapAll(tasks), timeout, unit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    return delegate.invokeAny(wrapAll(tasks));
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return delegate.invokeAny(wrapAll(tasks), timeout, unit);
  }

  // --- Lifecycle: delegate directly ---

  @Override
  public void shutdown() {
    delegate.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow() {
    return delegate.shutdownNow();
  }

  @Override
  public boolean isShutdown() {
    return delegate.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return delegate.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return delegate.awaitTermination(timeout, unit);
  }

  @Override
  public void close() {
    delegate.close();
  }

  // --- Internal ---

  private <T> List<Callable<T>> wrapAll(Collection<? extends Callable<T>> tasks) {
    return tasks.stream().map(task -> CallableContext.wrap(task, propagators)).toList();
  }
}
