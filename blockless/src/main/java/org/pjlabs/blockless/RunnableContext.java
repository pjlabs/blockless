package org.pjlabs.blockless;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Static helpers to compose {@link ContextPropagator} instances and wrap {@link Runnable} tasks.
 * <p>
 * Pass propagators (from {@code blockless-context-*} modules) in attach order; restore runs in
 * reverse order.
 */
public final class RunnableContext {

    private RunnableContext() {}

    /**
     * Captures a snapshot from each propagator.
     */
    private static Map<ContextPropagator, Object> captureSnapshots(List<ContextPropagator> propagators) {
        Objects.requireNonNull(propagators, "propagators");
        return List.copyOf(propagators)
            .stream()
            .map(Objects::requireNonNull)
            .map(p -> Map.entry(p, p.capture()))
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (a, b) -> {
                    throw new IllegalStateException("Duplicate propagator");
                },
                LinkedHashMap::new));
    }

    /**
     * Wraps a {@link Runnable}, capturing propagator state at wrap time.
     */
    public static Runnable wrap(Runnable runnable, ContextPropagator... propagators) {
        return wrap(runnable, List.of(propagators));
    }

    /**
     * Wraps a {@link Runnable}, capturing propagator state at wrap time.
     */
    public static Runnable wrap(Runnable runnable, List<ContextPropagator> propagators) {
        Objects.requireNonNull(runnable, "runnable");
        var snapshots = List.copyOf(captureSnapshots(propagators).entrySet());
        return () -> {
            var tokens = new Object[snapshots.size()];
            for (int i = 0; i < snapshots.size(); i++) {
                var entry = snapshots.get(i);
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
