package io.github.pjlabs.blockless;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Experimental integration point for optional supplier wait runtimes. Core has no continuation
 * dependency and retains its ordinary waiting behavior unless a handler is installed for the
 * current scope.
 */
public final class SupplierWaitSupport {
  private static final ThreadLocal<Consumer<AsyncSupplier<?>>> HANDLER = new ThreadLocal<>();

  private SupplierWaitSupport() {}

  /**
   * Runs {@code action} with a handler for unfinished platform-thread supplier reads, restoring the
   * previous handler on exit. Completed reads and virtual-thread reads bypass the handler. A
   * handler must return only when the supplier's outcome is readable, or throw; it must not call
   * get() on the unfinished supplier. Continuation runtimes can suspend here and return after
   * resumption.
   */
  public static <T> T withHandler(Consumer<AsyncSupplier<?>> handler, Supplier<T> action) {
    Objects.requireNonNull(handler, "handler");
    Objects.requireNonNull(action, "action");
    final var previous = HANDLER.get();
    HANDLER.set(handler);
    try {
      return action.get();
    } finally {
      if (previous == null) {
        HANDLER.remove();
      } else {
        HANDLER.set(previous);
      }
    }
  }

  static void await(AsyncSupplier<?> supplier) {
    if (Thread.currentThread().isVirtual() || supplier.isDone()) {
      return;
    }
    final var handler = HANDLER.get();
    if (handler != null) {
      handler.accept(supplier);
      if (!supplier.isDone()) {
        throw new IllegalStateException("Supplier wait handler returned before completion");
      }
    }
  }
}
