package org.pjlabs.blockless;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Parallel execution utility using virtual threads with context propagation.
 * <p>
 * Each task runs on its own virtual thread via {@link Thread#startVirtualThread},
 * with context captured at call time and propagated using the configured
 * {@link ContextPropagator} instances.
 *
 * <pre>{@code
 * var parallel = Parallel.create(new Slf4jMdcContextPropagator());
 * List<String> results = parallel.map(ids, id -> fetchName(id));
 * }</pre>
 */
public final class Parallel {

    private final List<ContextPropagator> propagators;

    private Parallel(List<ContextPropagator> propagators) {
        this.propagators = List.copyOf(propagators);
    }

    /**
     * Creates a {@link Parallel} instance with the given propagators.
     */
    public static Parallel create(ContextPropagator... propagators) {
        return create(List.of(propagators));
    }

    /**
     * Creates a {@link Parallel} instance with the given propagators.
     */
    public static Parallel create(List<ContextPropagator> propagators) {
        return new Parallel(propagators);
    }

    /**
     * Runs a supplier on a virtual thread with context propagation.
     * Returns a {@link Supplier} whose {@code get()} blocks until the result is available.
     */
    public <T> Supplier<T> async(Supplier<T> task) {
        Objects.requireNonNull(task, "task");
        return Blockless.supplier(CallableContext.wrap(task::get, propagators));
    }

    /**
     * Applies {@code fn} to each element on virtual threads with context propagation,
     * returning results in input order. Blocks until all tasks complete.
     */
    public <T, R> List<R> map(List<T> items, Function<T, R> fn) {
        Objects.requireNonNull(items, "items");
        Objects.requireNonNull(fn, "fn");

        var suppliers = items.stream()
                .map(item -> async(() -> fn.apply(item)))
                .toList();

        return suppliers.stream()
                .map(Supplier::get)
                .toList();
    }

    /**
     * Computes a value for each key on virtual threads with context propagation,
     * returning a map preserving key iteration order. Blocks until all tasks complete.
     */
    public <K, V> Map<K, V> asMap(Collection<K> keys, Function<K, V> valueMapper) {
        Objects.requireNonNull(keys, "keys");
        Objects.requireNonNull(valueMapper, "valueMapper");

        var entries = keys.stream()
                .map(key -> Map.entry(key, async(() -> valueMapper.apply(key))))
                .toList();

        var result = new LinkedHashMap<K, V>();
        for (var entry : entries) {
            result.put(entry.getKey(), entry.getValue().get());
        }
        return result;
    }
}
