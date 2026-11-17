package io.github.pjlabs.blockless;

import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

/** Stores a single outcome and waits for its publication before supplying it. */
final class OutcomeSupplier<T> implements Supplier<T> {

  private final CountDownLatch latch = new CountDownLatch(1);
  private T value;
  private Throwable failure;

  /** Called once by the producer; the latch publishes both fields to all waiting callers. */
  void complete(T value, Throwable failure) {
    this.value = value;
    this.failure = failure;
    latch.countDown();
  }

  @Override
  public T get() {
    if (latch.getCount() != 0) {
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
