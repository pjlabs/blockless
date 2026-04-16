package io.github.pjlabs.blockless;

/**
 * Captures thread-local context on one thread and restores it on another.
 *
 * <p>Implementations live in separate modules (e.g. {@code blockless-context-grpc}) so callers only
 * depend on the integrations they use.
 */
public interface ContextPropagator {

  /** Snapshot of the current thread's context for this propagator (may be {@code null}). */
  Object capture();

  /**
   * Applies {@code captured} on the current thread. Returns an opaque token for {@link
   * #restore(Object)}.
   */
  Object attach(Object captured);

  /** Restores the state from before {@link #attach(Object)} using the token returned by attach. */
  void restore(Object previous);
}
