package io.github.pjlabs.blockless;

import java.util.function.Supplier;

/**
 * A supplier with a cached asynchronous outcome. Used by optional continuation runtimes to avoid
 * waiting on a platform thread. Ordinary callers can keep using {@link Supplier}.
 */
public interface AsyncSupplier<T> extends Supplier<T> {
  /** Whether {@link #get()} can read the outcome without waiting for the worker. */
  boolean isDone();

  /**
   * Invokes {@code action} once the outcome is readable, including on failure. May invoke it inline
   * when already complete. The action must not block the completing thread.
   */
  void onComplete(Runnable action);
}
