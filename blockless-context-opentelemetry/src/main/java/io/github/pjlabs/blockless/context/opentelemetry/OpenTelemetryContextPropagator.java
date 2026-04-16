package io.github.pjlabs.blockless.context.opentelemetry;

import io.github.pjlabs.blockless.ContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;

/** Propagates OpenTelemetry {@link Context} across threads. */
public final class OpenTelemetryContextPropagator implements ContextPropagator {

  @Override
  public Object capture() {
    return Context.current();
  }

  @Override
  public Object attach(Object captured) {
    if (captured == null || !(captured instanceof Context context)) {
      return null;
    }
    return context.makeCurrent();
  }

  @Override
  public void restore(Object previous) {
    if (previous instanceof Scope scope) {
      scope.close();
    }
  }
}
