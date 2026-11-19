package io.github.pjlabs.blockless;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.function.Supplier;

/** Stores a single outcome and waits for its publication before supplying it. */
final class OutcomeSupplier<T> implements Supplier<T> {

  private final CompletableFuture<T> result = new CompletableFuture<>();
  private final CountDownLatch latch = new CountDownLatch(1);

  /** Called once by the producer; publishes the outcome to all current and later callers. */
  void complete(T value, Throwable failure) {
    try {
      if (failure == null) {
        result.complete(value);
      } else {
        // exceptionNow() unwraps one CompletionException; preserve the original failure.
        // The null message avoids calling the failure's toString.
        result.completeExceptionally(new CompletionException(null, failure));
      }
    } finally {
      latch.countDown();
    }
  }

  @Override
  public T get() {
    // Skip await() once completed, so an interrupted caller still gets the outcome.
    if (!result.isDone()) {
      try {
        latch.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
    return switch (result.state()) {
      case SUCCESS -> result.resultNow();
      case FAILED -> throw wrapIfChecked(result.exceptionNow());
      case CANCELLED, RUNNING -> throw new IllegalStateException("Result not completed");
    };
  }

  private static RuntimeException wrapIfChecked(Throwable t) {
    if (t instanceof RuntimeException runtimeException) {
      return runtimeException;
    }
    if (t instanceof Error error) {
      throw error;
    }
    return new RuntimeException(t);
  }
}
