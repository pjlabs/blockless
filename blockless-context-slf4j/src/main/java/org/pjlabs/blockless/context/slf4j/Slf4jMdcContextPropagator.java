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
        @SuppressWarnings("unchecked")
        var map = (Map<String, String>) captured;
        var previous = MDC.getCopyOfContextMap();
        if (map == null) {
            MDC.clear();
        } else {
            MDC.setContextMap(map);
        }
        return previous;
    }

    @Override
    public void restore(Object previous) {
        @SuppressWarnings("unchecked")
        var map = (Map<String, String>) previous;
        if (map == null) {
            MDC.clear();
        } else {
            MDC.setContextMap(map);
        }
    }
}
