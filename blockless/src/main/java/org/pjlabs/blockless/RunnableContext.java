package org.pjlabs.blockless;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Static helpers to compose {@link ContextPropagator} instances and wrap {@link Runnable} tasks.
 *
 * <p>Pass propagators (from {@code blockless-context-*} modules) in attach order; restore runs in
 * reverse order.
 */
public final class RunnableContext {

  private RunnableContext() {}

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

  /** Wraps a {@link Runnable}, capturing propagator state at wrap time. */
  public static Runnable wrap(Runnable runnable, ContextPropagator... propagators) {
    return wrap(runnable, List.of(propagators));
  }

  /** Wraps a {@link Runnable}, capturing propagator state at wrap time. */
  public static Runnable wrap(Runnable runnable, List<ContextPropagator> propagators) {
    Objects.requireNonNull(runnable, "runnable");
    final var snapshots = List.copyOf(captureSnapshots(propagators).entrySet());
    return () -> {
      final var tokens = new Object[snapshots.size()];
      for (int i = 0; i < snapshots.size(); i++) {
        final var entry = snapshots.get(i);
        tokens[i] = entry.getKey().attach(entry.getValue());
      }
      try {
        runnable.run();
      } finally {
        for (int i = snapshots.size() - 1; i >= 0; i--) {
          snapshots.get(i).getKey().restore(tokens[i]);
        }
      }
    };
  }
}
