package io.github.pjlabs.blockless;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Parallel execution utility using virtual threads with context propagation.
 *
 * <p>Each task runs on its own virtual thread via {@link Thread#startVirtualThread}, with context
 * captured at call time and propagated using the configured {@link ContextPropagator} instances.
 *
 * <p>When {@link #withMaxConcurrency(int)} is set, {@link #map} and {@link #toEither} use a sliding
 * window — at most N virtual threads are alive at any time. Without it, all tasks are launched
 * eagerly.
 *
 * <pre>{@code
 * var parallel = Parallel.create(new Slf4jMdcContextPropagator());
 * List<String> results = parallel.map(ids, id -> fetchName(id));
 * }</pre>
 */
public final class Parallel {

  private final List<ContextPropagator> propagators;
  private final int maxConcurrency;
  private final Semaphore semaphore;
  private final Duration timeout;

  private Parallel(List<ContextPropagator> propagators, int maxConcurrency, Duration timeout) {
    this.propagators = List.copyOf(propagators);
    this.maxConcurrency = maxConcurrency;
    this.semaphore = maxConcurrency > 0 ? new Semaphore(maxConcurrency) : null;
    this.timeout = timeout;
  }

  /** Creates an unbounded {@link Parallel} instance with the given propagators. */
  public static Parallel create(ContextPropagator... propagators) {
    return create(List.of(propagators));
  }

  /** Creates an unbounded {@link Parallel} instance with the given propagators. */
  public static Parallel create(List<ContextPropagator> propagators) {
    return new Parallel(propagators, 0, null);
  }

  /**
   * Returns a new {@link Parallel} with the same propagators but limited to {@code maxConcurrency}
   * concurrent tasks. In {@link #map} and {@link #toEither}, this controls the sliding window size
   * — at most N virtual threads are alive at any time.
   */
  public Parallel withMaxConcurrency(int maxConcurrency) {
    if (maxConcurrency < 1) {
      throw new IllegalArgumentException("maxConcurrency must be at least 1");
    }
    return new Parallel(propagators, maxConcurrency, timeout);
  }

  /**
   * Returns a new {@link Parallel} with a per-task timeout. If a task does not complete within the
   * duration, its thread is interrupted and a {@link TimeoutException} is thrown (wrapped in {@link
   * RuntimeException}).
   */
  public Parallel withTimeout(Duration timeout) {
    Objects.requireNonNull(timeout, "timeout");
    if (timeout.isNegative() || timeout.isZero()) {
      throw new IllegalArgumentException("timeout must be positive");
    }
    return new Parallel(propagators, maxConcurrency, timeout);
  }

  /**
   * Runs a supplier on a virtual thread with context propagation. Returns a {@link Supplier} whose
   * {@code get()} blocks until the result is available.
   */
  public <T> Supplier<T> async(Supplier<T> task) {
    Objects.requireNonNull(task, "task");
    Supplier<T> wrapped = task;
    if (semaphore != null) {
      wrapped = boundedSupplier(task, semaphore);
    }
    if (timeout != null) {
      wrapped = timedSupplier(wrapped);
    }
    return Blockless.supplier(CallableContext.wrap(wrapped::get, propagators));
  }

  /**
   * Applies {@code fn} to each element on virtual threads with context propagation, returning
   * results in input order. Blocks until all tasks complete.
   *
   * <p>With {@link #withMaxConcurrency(int)}, uses a sliding window — at most N virtual threads are
   * alive at any time. On failure, remaining in-progress tasks are cancelled.
   */
  public <T, R> List<R> map(List<T> items, Function<T, R> fn) {
    Objects.requireNonNull(items, "items");
    Objects.requireNonNull(fn, "fn");

    final var window = maxConcurrency > 0 ? maxConcurrency : items.size();
    final var results = new ArrayList<R>(items.size());
    final var wip = new ArrayDeque<VirtualTask<R>>(window);
    var success = false;

    try {
      for (final var item : items) {
        if (wip.size() >= window) {
          results.add(collectFirst(wip));
        }
        wip.addLast(startTask(() -> fn.apply(item)));
      }
      while (!wip.isEmpty()) {
        results.add(collectFirst(wip));
      }
      success = true;
      return results;
    } finally {
      if (!success) {
        cancelAndJoinAll(wip);
      }
    }
  }

  /**
   * Computes a value for each key on virtual threads with context propagation, returning a map
   * preserving key iteration order. Blocks until all tasks complete.
   */
  public <K, V> Map<K, V> asMap(Collection<K> keys, Function<K, V> valueMapper) {
    Objects.requireNonNull(keys, "keys");
    Objects.requireNonNull(valueMapper, "valueMapper");

    final var keyList = List.copyOf(keys);
    final var values = map(keyList, valueMapper::apply);

    final var result = new LinkedHashMap<K, V>();
    for (int i = 0; i < keyList.size(); i++) {
      result.put(keyList.get(i), values.get(i));
    }
    return result;
  }

  /**
   * Like {@link #map}, but collects per-item results instead of failing fast. Every task runs to
   * completion — no cancellation on failure. The returned list matches {@code items} in order; each
   * element is either {@link Either#ok(Object)} or {@link Either#fail(Object)}.
   */
  public <T, R> List<Either<R, Throwable>> toEither(List<T> items, Function<T, R> fn) {
    Objects.requireNonNull(items, "items");
    Objects.requireNonNull(fn, "fn");

    final var window = maxConcurrency > 0 ? maxConcurrency : items.size();
    final var results = new ArrayList<Either<R, Throwable>>(items.size());
    final var wip = new ArrayDeque<VirtualTask<R>>(window);

    for (final var item : items) {
      if (wip.size() >= window) {
        results.add(collectFirstAsEither(wip));
      }
      wip.addLast(startTask(() -> fn.apply(item)));
    }
    while (!wip.isEmpty()) {
      results.add(collectFirstAsEither(wip));
    }

    return List.copyOf(results);
  }

  /**
   * Like {@link #asMap}, but collects per-key results instead of failing fast. Every task runs to
   * completion. The returned map is keyed by {@code keys} with iteration order preserved.
   */
  public <K, V> Map<K, Either<V, Throwable>> toEitherMap(
      Collection<K> keys, Function<K, V> valueMapper) {
    Objects.requireNonNull(keys, "keys");
    Objects.requireNonNull(valueMapper, "valueMapper");

    final var keyList = List.copyOf(keys);
    final var eithers = toEither(keyList, valueMapper::apply);

    final var result = new LinkedHashMap<K, Either<V, Throwable>>();
    for (int i = 0; i < keyList.size(); i++) {
      result.put(keyList.get(i), eithers.get(i));
    }
    return Collections.unmodifiableMap(result);
  }

  // ── Internal task management ──

  private record VirtualTask<R>(
      Thread thread, AtomicReference<R> result, AtomicReference<Throwable> error) {}

  private <R> VirtualTask<R> startTask(Supplier<R> task) {
    final var result = new AtomicReference<R>();
    final var error = new AtomicReference<Throwable>();

    Supplier<R> wrapped = task;
    if (timeout != null) {
      wrapped = timedSupplier(wrapped);
    }
    final var finalWrapped = wrapped;

    final var callable = CallableContext.wrap((Callable<R>) finalWrapped::get, propagators);
    final var thread =
        Thread.startVirtualThread(
            () -> {
              try {
                result.set(callable.call());
              } catch (Exception e) {
                error.set(e);
              }
            });

    return new VirtualTask<>(thread, result, error);
  }

  private <R> R collectFirst(ArrayDeque<VirtualTask<R>> wip) {
    final var task = wip.pollFirst();
    joinTask(task);
    if (task.error().get() != null) {
      throw wrapIfNeeded(task.error().get());
    }
    return task.result().get();
  }

  private <R> Either<R, Throwable> collectFirstAsEither(ArrayDeque<VirtualTask<R>> wip) {
    final var task = wip.pollFirst();
    joinTask(task);
    if (task.error().get() != null) {
      final var cause = task.error().get();
      return Either.fail(
          cause instanceof RuntimeException re && re.getCause() != null ? re.getCause() : cause);
    }
    return Either.ok(task.result().get());
  }

  private <R> void joinTask(VirtualTask<R> task) {
    try {
      task.thread().join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private <R> void cancelAndJoinAll(ArrayDeque<VirtualTask<R>> wip) {
    for (final var task : wip) {
      task.thread().interrupt();
    }
    var wasInterrupted = false;
    for (final var task : wip) {
      while (true) {
        try {
          task.thread().join();
          break;
        } catch (InterruptedException e) {
          wasInterrupted = true;
        }
      }
    }
    if (wasInterrupted) {
      Thread.currentThread().interrupt();
    }
    wip.clear();
  }

  private static RuntimeException wrapIfNeeded(Throwable t) {
    if (t instanceof RuntimeException re) {
      return re;
    }
    return new RuntimeException(t);
  }

  private <T> Supplier<T> timedSupplier(Supplier<T> task) {
    return () -> {
      final var taskThread = Thread.currentThread();
      final var done = new AtomicBoolean(false);
      final var timer =
          Thread.startVirtualThread(
              () -> {
                try {
                  Thread.sleep(timeout);
                  if (!done.get()) {
                    taskThread.interrupt();
                  }
                } catch (InterruptedException ignored) {
                }
              });
      try {
        return task.get();
      } finally {
        done.set(true);
        timer.interrupt();
      }
    };
  }

  private <T> Supplier<T> boundedSupplier(Supplier<T> task, Semaphore semaphore) {
    return () -> {
      semaphore.acquireUninterruptibly();
      try {
        return task.get();
      } finally {
        semaphore.release();
      }
    };
  }
}
