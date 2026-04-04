package org.pjlabs.blockless.context.slf4j;

import java.util.Map;
import org.pjlabs.blockless.ContextPropagator;
import org.slf4j.MDC;

/**
 * Propagates SLF4J {@link MDC} across threads.
 */
public final class Slf4jMdcContextPropagator implements ContextPropagator {

    @Override
    public Object capture() {
        return MDC.getCopyOfContextMap();
    }

    @Override
    public Object attach(Object captured) {
        if (captured == null || !(captured instanceof Map<?, ?> raw)) {
            return null;
        }
        @SuppressWarnings("unchecked")
        Map<String, String> map = (Map<String, String>) raw;
        final var previous = MDC.getCopyOfContextMap();
        MDC.setContextMap(map);
        return previous;
    }

    /**
     * Restores the MDC snapshot from {@link #attach(Object)}. When {@code previous} is not a
     * {@code Map<String, String>} (including {@code null}), clears MDC so the worker thread does not
     * leak context; callers should only pass values returned from {@code attach} for a successful attach.
     */
    @Override
    public void restore(Object previous) {
        if (previous == null || !(previous instanceof Map<?, ?> raw)) {
            MDC.clear();
            return;
        }
        @SuppressWarnings("unchecked")
        Map<String, String> map = (Map<String, String>) raw;
        MDC.setContextMap(map);
    }
}
