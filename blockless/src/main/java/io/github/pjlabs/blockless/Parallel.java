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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Parallel execution utility using virtual threads with context propagation.
 *
 * <p>Each task runs on its own virtual thread via {@link Thread#startVirtualThread}, with context
 * captured at call time and propagated using the configured {@link ContextPropagator} instances.
 *
 * <p>When {@link #withMaxConcurrency(int)} is set, {@link #map} and {@link #toEither} use a sliding
 * window — at most N worker threads are alive per batch. Tasks are collected in completion order to
 * avoid head-of-line blocking, but results are always returned in input order.
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
   * — at most N worker threads are alive per batch.
   */
  public Parallel withMaxConcurrency(int maxConcurrency) {
    if (maxConcurrency < 1) {
      throw new IllegalArgumentException("maxConcurrency must be at least 1");
    }
    return new Parallel(propagators, maxConcurrency, timeout);
  }

  /**
   * Returns a new {@link Parallel} with a per-task timeout. If a task does not complete within the
   * duration, its worker is interrupted and its result fails with {@link TimeoutException}. {@link
   * #map} and {@link #async} wrap it in {@link RuntimeException}; {@link #toEither} collects it as
   * a failed {@link Either}.
   *
   * <p>Timeouts stop waiting for the result, even if the task ignores interruption. Such tasks may
   * continue running in the background. In bounded batches they retain their concurrency slot until
   * they exit, so queued tasks may still delay completion of the batch.
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
    return startTask(task, (value, failure) -> {}, () -> {}).supplier();
  }

  /**
   * Applies {@code fn} to each element on virtual threads with context propagation, returning
   * results in input order. Blocks until all tasks complete.
   *
   * <p>With {@link #withMaxConcurrency(int)}, uses a sliding window — at most N worker threads are
   * alive per batch. Tasks are collected in completion order to maximize throughput. On failure,
   * remaining in-progress tasks are interrupted without waiting for them to exit.
   */
  @SuppressWarnings("unchecked")
  public <T, R> List<R> map(List<T> items, Function<T, R> fn) {
    Objects.requireNonNull(items, "items");
    Objects.requireNonNull(fn, "fn");
    if (items.isEmpty()) {
      return List.of();
    }

    final var results = (R[]) new Object[items.size()];
    runBatch(
        items,
        fn,
        completed -> {
          if (completed.failure() != null) {
            throw wrapIfNeeded(completed.failure());
          }
          results[completed.index()] = completed.result();
        });
    return Collections.unmodifiableList(Arrays.asList(results));
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
   * Like {@link #map}, but collects per-item results instead of failing fast. Every item produces a
   * result or timeout — no cancellation of other tasks on failure. The returned list matches {@code
   * items} in order; each element is either {@link Either#ok(Object)} or {@link
   * Either#fail(Object)}.
   */
  @SuppressWarnings("unchecked")
  public <T, R> List<Either<R, Throwable>> toEither(List<T> items, Function<T, R> fn) {
    Objects.requireNonNull(items, "items");
    Objects.requireNonNull(fn, "fn");
    if (items.isEmpty()) {
      return List.of();
    }

    final var results = (Either<R, Throwable>[]) new Either[items.size()];
    runBatch(
        items,
        fn,
        completed -> {
          final var cause = completed.failure();
          results[completed.index()] =
              cause == null
                  ? Either.ok(completed.result())
                  : Either.fail(
                      cause instanceof RuntimeException re && re.getCause() != null
                          ? re.getCause()
                          : cause);
        });
    return Collections.unmodifiableList(Arrays.asList(results));
  }

  /**
   * Like {@link #asMap}, but collects per-key results instead of failing fast. Every item produces
   * a result or timeout. The returned map is keyed by {@code keys} with iteration order preserved.
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

  // A timeout can produce a result before worker exit; only exit releases a batch slot.
  private sealed interface Signal<R> {
    record Completed<R>(int index, R result, Throwable failure) implements Signal<R> {}

    record Exited<R>(int index) implements Signal<R> {}
  }

  /**
   * Runs {@code fn} over {@code items} in a sliding window of at most {@code maxConcurrency} live
   * workers, passing each completed result to {@code onResult}. If anything throws, including
   * {@code onResult}, all started tasks are cancelled.
   */
  private <T, R> void runBatch(
      List<T> items, Function<T, R> fn, Consumer<Signal.Completed<R>> onResult) {
    final var totalTaskCount = items.size();
    final var windowSize = maxConcurrency > 0 ? maxConcurrency : totalTaskCount;
    final var completionQueue = new LinkedBlockingQueue<Signal<R>>();
    final var tasks = new ArrayList<VirtualTask<R>>(Collections.nCopies(totalTaskCount, null));
    var activeTaskCount = 0;
    var nextTaskIndex = 0;
    var completedTaskCount = 0;
    var success = false;

    try {
      while (completedTaskCount < totalTaskCount) {
        while (nextTaskIndex < totalTaskCount && activeTaskCount < windowSize) {
          tasks.set(
              nextTaskIndex,
              startTask(nextTaskIndex, items.get(nextTaskIndex), fn, completionQueue));
          nextTaskIndex++;
          activeTaskCount++;
        }

        switch (takeFromQueue(completionQueue)) {
          case Signal.Exited<R> exited -> {
            joinTask(tasks.get(exited.index()));
            activeTaskCount--;
          }
          case Signal.Completed<R> completed -> {
            onResult.accept(completed);
            completedTaskCount++;
          }
        }
      }
      success = true;
    } finally {
      if (!success) {
        cancelAll(tasks);
      }
    }
  }

  private record VirtualTask<R>(Thread worker, CompletableFuture<R> result) {
    void cancel() {
      worker.interrupt();
      result.cancel(false);
    }

    Supplier<R> supplier() {
      final var supplier = Blockless.supplier(result);
      return () -> {
        try {
          return supplier.get();
        } finally {
          if (Thread.currentThread().isInterrupted()) {
            cancel();
          }
        }
      };
    }
  }

  private <T, R> VirtualTask<R> startTask(
      int index, T item, Function<T, R> fn, LinkedBlockingQueue<Signal<R>> completionQueue) {
    return startTask(
        () -> fn.apply(item),
        (value, failure) -> completionQueue.add(new Signal.Completed<>(index, value, failure)),
        () -> completionQueue.add(new Signal.Exited<>(index)));
  }

  private <T> VirtualTask<T> startTask(
      Supplier<T> task, BiConsumer<T, Throwable> onResult, Runnable onWorkerExit) {
    final var timeoutNanos = timeout == null ? 0 : timeout.toNanos();
    final var result = new CompletableFuture<T>();
    final var callable = CallableContext.<T>wrap(task::get, propagators);
    final var worker =
        Thread.ofVirtual()
            .unstarted(
                () -> {
                  try {
                    result.complete(callable.call());
                  } catch (Throwable failure) {
                    result.completeExceptionally(failure);
                  } finally {
                    onWorkerExit.run();
                  }
                });

    result.whenComplete(
        (value, failure) -> {
          if (failure instanceof TimeoutException) {
            worker.interrupt();
          }
          onResult.accept(value, failure);
        });

    worker.start();
    if (timeout != null) {
      result.orTimeout(timeoutNanos, TimeUnit.NANOSECONDS);
    }
    return new VirtualTask<>(worker, result);
  }

  private static <R> Signal<R> takeFromQueue(LinkedBlockingQueue<Signal<R>> queue) {
    try {
      return queue.take();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private <R> void joinTask(VirtualTask<R> task) {
    try {
      task.worker().join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  private <R> void cancelAll(List<VirtualTask<R>> tasks) {
    for (final var task : tasks) {
      if (task != null) {
        task.cancel();
      }
    }
  }

  private static RuntimeException wrapIfNeeded(Throwable t) {
    if (t instanceof RuntimeException re) {
      return re;
    }
    return new RuntimeException(t);
  }
}
