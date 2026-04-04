package org.pjlabs.blockless;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * Static helpers to compose {@link ContextPropagator} instances and wrap {@link Callable} tasks.
 *
 * <p>Pass propagators (from {@code blockless-context-*} modules) in attach order; restore runs in
 * reverse order.
 */
public final class CallableContext {

  private CallableContext() {}

  /** Captures a snapshot from each propagator (used by {@link #wrap(Callable, List)}). */
  private static Map<ContextPropagator, Object> captureSnapshots(ContextPropagator... propagators) {
    return captureSnapshots(List.of(propagators));
  }

  /** Captures a snapshot from each propagator. */
  private static Map<ContextPropagator, Object> captureSnapshots(
      List<ContextPropagator> propagators) {
    Objects.requireNonNull(propagators, "propagators");
    final var snapshots = new LinkedHashMap<ContextPropagator, Object>();
    for (var p : List.copyOf(propagators)) {
      Objects.requireNonNull(p, "propagator");
      if (snapshots.containsKey(p)) {
        throw new IllegalStateException("Duplicate propagator");
      }
      snapshots.put(p, p.capture());
    }
    return snapshots;
  }

  /** Wraps a {@link Callable}, capturing propagator state at wrap time. */
  public static <T> Callable<T> wrap(Callable<T> callable, ContextPropagator... propagators) {
    return wrap(callable, List.of(propagators));
  }

  /** Wraps a {@link Callable}, capturing propagator state at wrap time. */
  public static <T> Callable<T> wrap(Callable<T> callable, List<ContextPropagator> propagators) {
    Objects.requireNonNull(callable, "callable");
    final var snapshots = List.copyOf(captureSnapshots(propagators).entrySet());
    return () -> {
      final var tokens = new Object[snapshots.size()];
      for (int i = 0; i < snapshots.size(); i++) {
        final var entry = snapshots.get(i);
        tokens[i] = entry.getKey().attach(entry.getValue());
      }
      try {
        return callable.call();
      } finally {
        for (int i = snapshots.size() - 1; i >= 0; i--) {
          snapshots.get(i).getKey().restore(tokens[i]);
        }
      }
    };
  }
}
