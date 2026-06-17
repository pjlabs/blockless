package io.github.pjlabs.blockless;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
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
 * window — at most N virtual threads are alive at any time. Tasks are collected in completion order
 * to avoid head-of-line blocking, but results are always returned in input order.
 *
 * <pre>{@code
 * var parallel = Parallel.create(new Slf4jMdcContextPropagator());
 * List<String> results = parallel.map(ids, id -> fetchName(id));
 * }</pre>
 */
public final class Parallel {

  private final List<ContextPropagator> propagators;
  private final int maxConcurrency;
  private final Duration timeout;

  private Parallel(List<ContextPropagator> propagators, int maxConcurrency, Duration timeout) {
    this.propagators = List.copyOf(propagators);
    this.maxConcurrency = maxConcurrency;
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
   *
   * <p>{@link #withMaxConcurrency(int)} does not affect this method — it only controls the sliding
   * window in {@link #map} and {@link #toEither}. For bounded batch work, use {@link #map}.
   */
  public <T> Supplier<T> async(Supplier<T> task) {
    Objects.requireNonNull(task, "task");
    Supplier<T> wrapped = task;
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
   * alive at any time. Tasks are collected in completion order to maximize throughput. On failure,
   * remaining in-progress tasks are cancelled.
   */
  @SuppressWarnings("unchecked")
  public <T, R> List<R> map(List<T> items, Function<T, R> fn) {
    Objects.requireNonNull(items, "items");
    Objects.requireNonNull(fn, "fn");
    if (items.isEmpty()) {
      return List.of();
    }

    final var totalTaskCount = items.size();
    final var windowSize = maxConcurrency > 0 ? maxConcurrency : totalTaskCount;
    final var completionQueue = new LinkedBlockingQueue<Integer>();
    final var tasks = new ArrayList<VirtualTask<R>>(Collections.nCopies(totalTaskCount, null));
    final var results = (R[]) new Object[totalTaskCount];
    var activeTaskCount = 0;
    var nextTaskIndex = 0;
    var completedTaskCount = 0;
    var success = false;

    try {
      while (nextTaskIndex < totalTaskCount && activeTaskCount < windowSize) {
        tasks.set(
            nextTaskIndex, startTask(nextTaskIndex, items.get(nextTaskIndex), fn, completionQueue));
        nextTaskIndex++;
        activeTaskCount++;
      }

      while (completedTaskCount < totalTaskCount) {
        final var completedTaskIndex = takeFromQueue(completionQueue);
        final var task = tasks.get(completedTaskIndex);
        joinTask(task);
        if (task.error().get() != null) {
          throw wrapIfNeeded(task.error().get());
        }
        results[completedTaskIndex] = task.result().get();
        completedTaskCount++;
        activeTaskCount--;

        if (nextTaskIndex < totalTaskCount) {
          tasks.set(
              nextTaskIndex,
              startTask(nextTaskIndex, items.get(nextTaskIndex), fn, completionQueue));
          nextTaskIndex++;
          activeTaskCount++;
        }
      }

      success = true;
      return Collections.unmodifiableList(Arrays.asList(results));
    } finally {
      if (!success) {
        cancelAndJoinAll(tasks);
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
  @SuppressWarnings("unchecked")
  public <T, R> List<Either<R, Throwable>> toEither(List<T> items, Function<T, R> fn) {
    Objects.requireNonNull(items, "items");
    Objects.requireNonNull(fn, "fn");
    if (items.isEmpty()) {
      return List.of();
    }

    final var totalTaskCount = items.size();
    final var windowSize = maxConcurrency > 0 ? maxConcurrency : totalTaskCount;
    final var completionQueue = new LinkedBlockingQueue<Integer>();
    final var tasks = new ArrayList<VirtualTask<R>>(Collections.nCopies(totalTaskCount, null));
    final var results = (Either<R, Throwable>[]) new Either[totalTaskCount];
    var activeTaskCount = 0;
    var nextTaskIndex = 0;
    var completedTaskCount = 0;
    var success = false;

    try {
      while (nextTaskIndex < totalTaskCount && activeTaskCount < windowSize) {
        tasks.set(
            nextTaskIndex, startTask(nextTaskIndex, items.get(nextTaskIndex), fn, completionQueue));
        nextTaskIndex++;
        activeTaskCount++;
      }

      while (completedTaskCount < totalTaskCount) {
        final var completedTaskIndex = takeFromQueue(completionQueue);
        final var task = tasks.get(completedTaskIndex);
        joinTask(task);
        if (task.error().get() != null) {
          final var cause = task.error().get();
          results[completedTaskIndex] =
              Either.fail(
                  cause instanceof RuntimeException re && re.getCause() != null
                      ? re.getCause()
                      : cause);
        } else {
          results[completedTaskIndex] = Either.ok(task.result().get());
        }
        completedTaskCount++;
        activeTaskCount--;

        if (nextTaskIndex < totalTaskCount) {
          tasks.set(
              nextTaskIndex,
              startTask(nextTaskIndex, items.get(nextTaskIndex), fn, completionQueue));
          nextTaskIndex++;
          activeTaskCount++;
        }
      }

      success = true;
      return Collections.unmodifiableList(Arrays.asList(results));
    } finally {
      if (!success) {
        cancelAndJoinAll(tasks);
      }
    }
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

  private <T, R> VirtualTask<R> startTask(
      int index, T item, Function<T, R> fn, LinkedBlockingQueue<Integer> completionQueue) {
    final var result = new AtomicReference<R>();
    final var error = new AtomicReference<Throwable>();

    Supplier<R> task = () -> fn.apply(item);
    if (timeout != null) {
      task = timedSupplier(task);
    }
    final var finalTask = task;

    final var callable = CallableContext.wrap((Callable<R>) finalTask::get, propagators);
    final var thread =
        Thread.startVirtualThread(
            () -> {
              try {
                result.set(callable.call());
              } catch (Exception e) {
                error.set(e);
              } finally {
                completionQueue.add(index);
              }
            });

    return new VirtualTask<>(thread, result, error);
  }

  private static int takeFromQueue(LinkedBlockingQueue<Integer> queue) {
    try {
      return queue.take();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private <R> void joinTask(VirtualTask<R> task) {
    try {
      task.thread().join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private <R> void cancelAndJoinAll(List<VirtualTask<R>> tasks) {
    for (final var task : tasks) {
      if (task != null) {
        task.thread().interrupt();
      }
    }
    var wasInterrupted = false;
    for (final var task : tasks) {
      if (task != null) {
        while (true) {
          try {
            task.thread().join();
            break;
          } catch (InterruptedException e) {
            wasInterrupted = true;
          }
        }
      }
    }
    if (wasInterrupted) {
      Thread.currentThread().interrupt();
    }
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
}
