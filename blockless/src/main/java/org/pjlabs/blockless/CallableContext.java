package org.pjlabs.blockless;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * Composes {@link ContextPropagator} instances to wrap {@link Callable} tasks.
 * <p>
 * Pass the propagators you need (from {@code blockless-context-*} modules) in the order you want
 * attach to run; restore runs in reverse order.
 */
public final class CallableContext {

    private final List<ContextPropagator> propagators;
    private final List<Object> capturedSnapshots;

    private CallableContext(List<ContextPropagator> propagators, List<Object> capturedSnapshots) {
        this.propagators = propagators;
        this.capturedSnapshots = capturedSnapshots;
    }

    /**
     * Captures using the given propagators (typically paired with {@link #wrapCaptured(Callable)}).
     */
    public static CallableContext capture(ContextPropagator... propagators) {
        return capture(List.of(propagators));
    }

    /**
     * Captures using the given propagators.
     */
    public static CallableContext capture(List<ContextPropagator> propagators) {
        Objects.requireNonNull(propagators, "propagators");
        var list = List.copyOf(propagators);
        var snapshots = new ArrayList<Object>(list.size());
        for (var p : list) {
            Objects.requireNonNull(p, "propagator");
            snapshots.add(p.capture());
        }
        return new CallableContext(list, List.copyOf(snapshots));
    }

    /**
     * Wraps a {@link Callable}, capturing propagators at wrap time.
     */
    public static <T> Callable<T> wrap(Callable<T> callable, ContextPropagator... propagators) {
        return wrap(callable, List.of(propagators));
    }

    /**
     * Wraps a {@link Callable}, capturing propagators at wrap time.
     */
    public static <T> Callable<T> wrap(Callable<T> callable, List<ContextPropagator> propagators) {
        Objects.requireNonNull(callable, "callable");
        return capture(propagators).wrapCaptured(callable);
    }

    /**
     * Wraps a {@link Callable} using this instance's captured snapshots.
     */
    public <T> Callable<T> wrapCaptured(Callable<T> callable) {
        Objects.requireNonNull(callable, "callable");
        return () -> {
            var previous = new ArrayList<Object>(propagators.size());
            for (int i = 0; i < propagators.size(); i++) {
                previous.add(propagators.get(i).attach(capturedSnapshots.get(i)));
            }
            try {
                return callable.call();
            } finally {
                for (int i = propagators.size() - 1; i >= 0; i--) {
                    propagators.get(i).restore(previous.get(i));
                }
            }
        };
    }
}
