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
        if (captured == null || !(captured instanceof Map map)) {
            return null;
        }
        var previous = MDC.getCopyOfContextMap();
        MDC.setContextMap(map);   
        return previous;
    }

    @Override
    public void restore(Object previous) {
        if (previous == null || !(previous instanceof Map map)) {
            MDC.clear();
            return;
        }
        MDC.setContextMap(map);
    }
}
