package org.pjlabs.blockless.context.opentelemetry;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.pjlabs.blockless.ContextPropagator;

/**
 * Propagates OpenTelemetry {@link Context} across threads.
 */
public final class OpenTelemetryContextPropagator implements ContextPropagator {

    @Override
    public Object capture() {
        return Context.current();
    }

    @Override
    public Object attach(Object captured) {
        if (captured == null) {
            return null;
        }
        return ((Context) captured).makeCurrent();
    }

    @Override
    public void restore(Object previous) {
        if (previous instanceof Scope scope) {
            scope.close();
        }
    }
}
