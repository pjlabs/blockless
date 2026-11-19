package io.github.pjlabs.blockless;

import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

/** Stores a single outcome and waits for its publication before supplying it. */
final class OutcomeSupplier<T> implements Supplier<T> {

  private final CountDownLatch latch = new CountDownLatch(1);
  private T value;
  private Throwable failure;
  // Written after value and failure: a caller that reads true also sees both fields.
  private volatile boolean completed;

  /** Called once by the producer; publishes the outcome to all current and later callers. */
  void complete(T value, Throwable failure) {
    this.value = value;
    this.failure = failure;
    completed = true;
    latch.countDown();
  }

  @Override
  public T get() {
    // Skip await() once completed, so an interrupted caller still gets the outcome.
    if (!completed) {
      try {
        latch.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
    if (failure instanceof RuntimeException runtimeException) {
      throw runtimeException;
    }
    if (failure instanceof Error error) {
      throw error;
    }
    if (failure != null) {
      throw new RuntimeException(failure);
    }
    return value;
  }
}
