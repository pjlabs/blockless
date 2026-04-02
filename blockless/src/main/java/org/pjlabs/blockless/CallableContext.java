package org.pjlabs.blockless;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

/**
 * Static helpers to compose {@link ContextPropagator} instances and wrap {@link Callable} tasks.
 * <p>
 * Pass propagators (from {@code blockless-context-*} modules) in attach order; restore runs in
 * reverse order.
 */
public final class CallableContext {

    private CallableContext() {}

    /**
     * Captures a snapshot from each propagator (e.g. to pair with {@link #wrapWithSnapshots}).
     */
    private static Map<ContextPropagator, Object> captureSnapshots(ContextPropagator... propagators) {
        return captureSnapshots(List.of(propagators));
    }

    /**
     * Captures a snapshot from each propagator.
     */
    private static Map<ContextPropagator, Object> captureSnapshots(List<ContextPropagator> propagators) {
        Objects.requireNonNull(propagators, "propagators");
        return List.copyOf(propagators)
            .stream()
            .map(Objects::requireNonNull)
            .map(p -> Map.entry(p, p.capture()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * Wraps a {@link Callable}, capturing propagator state at wrap time.
     */
    public static <T> Callable<T> wrap(Callable<T> callable, ContextPropagator... propagators) {
        return wrap(callable, List.of(propagators));
    }

    /**
     * Wraps a {@link Callable}, capturing propagator state at wrap time.
     */
    public static <T> Callable<T> wrap(Callable<T> callable, List<ContextPropagator> propagators) {
        Objects.requireNonNull(callable, "callable");
        var snapshots = captureSnapshots(propagators);
        return () -> {
            snapshots.forEach((p, ctx) -> p.attach(ctx));
            try {
                return callable.call();
            } finally {
                snapshots.forEach((p, ctx) -> p.restore(ctx));
            }
        };
    }

}
