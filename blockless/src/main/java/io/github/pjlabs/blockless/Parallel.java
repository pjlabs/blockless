package io.github.pjlabs.blockless;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Parallel execution utility using virtual threads with context propagation.
 *
 * <p>Each task runs on its own virtual thread via {@link Thread#startVirtualThread}, with context
 * captured at call time and propagated using the configured {@link ContextPropagator} instances.
 *
 * <pre>{@code
 * var parallel = Parallel.create(new Slf4jMdcContextPropagator());
 * List<String> results = parallel.map(ids, id -> fetchName(id));
 * }</pre>
 */
public final class Parallel {

  private final List<ContextPropagator> propagators;
  private final Semaphore semaphore;
  private final Duration timeout;

  private Parallel(List<ContextPropagator> propagators, Semaphore semaphore, Duration timeout) {
    this.propagators = List.copyOf(propagators);
    this.semaphore = semaphore;
    this.timeout = timeout;
  }

  /** Creates an unbounded {@link Parallel} instance with the given propagators. */
  public static Parallel create(ContextPropagator... propagators) {
    return create(List.of(propagators));
  }

  /** Creates an unbounded {@link Parallel} instance with the given propagators. */
  public static Parallel create(List<ContextPropagator> propagators) {
    return new Parallel(propagators, null, null);
  }

  /**
   * Returns a new {@link Parallel} with the same propagators but limited to {@code maxConcurrency}
   * concurrent tasks. Tasks beyond the limit park on virtual threads until a permit is available.
   */
  public Parallel withMaxConcurrency(int maxConcurrency) {
    if (maxConcurrency < 1) {
      throw new IllegalArgumentException("maxConcurrency must be at least 1");
    }
    return new Parallel(propagators, new Semaphore(maxConcurrency), timeout);
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
    return new Parallel(propagators, semaphore, timeout);
  }

  /**
   * Runs a supplier on a virtual thread with context propagation. Returns a {@link Supplier} whose
   * {@code get()} blocks until the result is available.
   */
  public <T> Supplier<T> async(Supplier<T> task) {
    Objects.requireNonNull(task, "task");
    var wrappedSupplier = semaphore != null ? boundedSupplier(task) : task;
    if (timeout != null) {
      wrappedSupplier = timedSupplier(wrappedSupplier);
    }
    return Blockless.supplier(CallableContext.wrap(wrappedSupplier::get, propagators));
  }

  private <T> Supplier<T> timedSupplier(Supplier<T> task) {
    return () -> {
      final var taskThread = Thread.currentThread();
      final var timer =
          Thread.startVirtualThread(
              () -> {
                try {
                  Thread.sleep(timeout);
                  taskThread.interrupt();
                } catch (InterruptedException ignored) {
                  // Timer cancelled — task completed in time
                }
              });
      try {
        return task.get();
      } finally {
        timer.interrupt();
      }
    };
  }

  private <T> Supplier<T> boundedSupplier(Supplier<T> task) {
    return () -> {
      semaphore.acquireUninterruptibly();
      try {
        return task.get();
      } finally {
        semaphore.release();
      }
    };
  }

  /**
   * Applies {@code fn} to each element on virtual threads with context propagation, returning
   * results in input order. Blocks until all tasks complete.
   */
  public <T, R> List<R> map(List<T> items, Function<T, R> fn) {
    Objects.requireNonNull(items, "items");
    Objects.requireNonNull(fn, "fn");

    final var suppliers = items.stream().map(item -> async(() -> fn.apply(item))).toList();

    return suppliers.stream().map(Supplier::get).toList();
  }

  /**
   * Computes a value for each key on virtual threads with context propagation, returning a map
   * preserving key iteration order. Blocks until all tasks complete.
   */
  public <K, V> Map<K, V> asMap(Collection<K> keys, Function<K, V> valueMapper) {
    Objects.requireNonNull(keys, "keys");
    Objects.requireNonNull(valueMapper, "valueMapper");

    final var entries =
        keys.stream().map(key -> Map.entry(key, async(() -> valueMapper.apply(key)))).toList();

    final var result = new LinkedHashMap<K, V>();
    for (var entry : entries) {
      result.put(entry.getKey(), entry.getValue().get());
    }
    return result;
  }

  /**
   * Like {@link #map}, but collects per-item results instead of failing fast. Every task runs to
   * completion. The returned list matches {@code items} in order; each element is either {@link
   * Either#ok(Object)} or {@link Either#fail(Object)}. The cause is unwrapped from {@link
   * RuntimeException} when present.
   */
  public <T, R> List<Either<R, Throwable>> toEither(List<T> items, Function<T, R> fn) {
    Objects.requireNonNull(items, "items");
    Objects.requireNonNull(fn, "fn");

    final var suppliers = items.stream().map(item -> async(() -> fn.apply(item))).toList();

    final var results = new ArrayList<Either<R, Throwable>>(suppliers.size());

    for (final var supplier : suppliers) {
      try {
        results.add(Either.ok(supplier.get()));
      } catch (final RuntimeException e) {
        results.add(Either.fail(e.getCause() != null ? e.getCause() : e));
      }
    }

    return List.copyOf(results);
  }

  /**
   * Like {@link #asMap}, but collects per-key results instead of failing fast. Every task runs to
   * completion. The returned map is keyed by {@code keys} with iteration order preserved; each
   * value is either {@link Either#ok(Object)} or {@link Either#fail(Object)}. The cause is
   * unwrapped from {@link RuntimeException} when present.
   */
  public <K, V> Map<K, Either<V, Throwable>> toEitherMap(
      Collection<K> keys, Function<K, V> valueMapper) {
    Objects.requireNonNull(keys, "keys");
    Objects.requireNonNull(valueMapper, "valueMapper");

    final var entries =
        keys.stream().map(key -> Map.entry(key, async(() -> valueMapper.apply(key)))).toList();

    final var result = new LinkedHashMap<K, Either<V, Throwable>>();

    for (final var entry : entries) {
      try {
        result.put(entry.getKey(), Either.ok(entry.getValue().get()));
      } catch (final RuntimeException e) {
        result.put(entry.getKey(), Either.fail(e.getCause() != null ? e.getCause() : e));
      }
    }

    return Collections.unmodifiableMap(new LinkedHashMap<>(result));
  }
}
