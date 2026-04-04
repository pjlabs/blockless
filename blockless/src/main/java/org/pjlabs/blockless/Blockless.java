package org.pjlabs.blockless;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class Blockless {

  private static final System.Logger LOGGER = System.getLogger(Blockless.class.getName());

  /** Logs a friendly startup banner. Call once at application start if you like. */
  public static void roar() {
    LOGGER.log(
        System.Logger.Level.INFO,
        "\uD83D\uDC09 blockless is ready — no platform threads were harmed");
  }

  /**
   * Returns a supplier that will block until the completion stage is complete. Use when you want to
   * eagerly run the completion stage now and get the result later in execution.
   *
   * @param stage the completion stage
   * @param <T> the type of the value
   * @return a supplier that will block until the completion stage is complete
   */
  public static <T> Supplier<T> supplier(CompletionStage<T> stage) {
    final var result = new AtomicReference<T>();
    final var throwable = new AtomicReference<Throwable>();
    final var latch = new CountDownLatch(1);
    stage.whenComplete(
        (T v, Throwable t) -> {
          if (t != null) {
            throwable.set(t);
          } else {
            result.set(v);
          }
          latch.countDown();
        });
    final var thread =
        Thread.startVirtualThread(
            () -> {
              try {
                latch.await();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throwable.set(e);
              }
            });
    return () -> {
      try {
        thread.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throwable.set(e);
      }
      if (throwable.get() != null) {
        throw new RuntimeException(throwable.get());
      }
      return result.get();
    };
  }

  /**
   * Returns the value of the completion stage. Use when you want to run the completion stage now
   * and get the result before continuing execution.
   *
   * @param stage the completion stage
   * @param <T> the type of the value
   * @return the value of the completion stage
   */
  public static <T> T get(CompletionStage<T> stage) {
    return supplier(stage).get();
  }

  /**
   * Runs the callable in a virtual thread and return the value of the callable.
   *
   * @param callable a callable
   * @param <T> the type of the value
   * @return the value of the callable
   */
  public static <T> Supplier<T> supplier(Callable<T> callable) {
    final var result = new AtomicReference<T>();
    final var throwable = new AtomicReference<Throwable>();
    final var thread =
        Thread.startVirtualThread(
            () -> {
              try {
                result.set(callable.call());
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throwable.set(e);
              } catch (Exception e) {
                throwable.set(e);
              }
            });
    return () -> {
      try {
        thread.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throwable.set(e);
      }
      if (throwable.get() != null) {
        throw new RuntimeException(throwable.get());
      }
      return result.get();
    };
  }

  /**
   * Returns the value of the callable. Use when you want to run the callable now and get the result
   * before continuing execution.
   *
   * @param callable the callable
   * @param <T> the type of the value
   * @return the value of the callable
   */
  public static <T> T get(Callable<T> callable) {
    return supplier(callable).get();
  }
}
