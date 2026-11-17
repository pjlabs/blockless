package io.github.pjlabs.blockless;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
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
   * <p>An interruption while waiting affects only that invocation.
   *
   * @param stage the completion stage
   * @param <T> the type of the value
   * @return a supplier that will block until the completion stage is complete
   */
  public static <T> Supplier<T> supplier(CompletionStage<T> stage) {
    final var outcome = new OutcomeSupplier<T>();
    stage.whenComplete(outcome::complete);
    return outcome;
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
   * Starts the callable on a virtual thread and returns a supplier that waits for its completion.
   *
   * <p>Once completed, the supplier reads the cached outcome without waiting, even if the caller is
   * interrupted. An interruption while waiting affects only that invocation.
   *
   * @param callable a callable
   * @param <T> the type of the value
   * @return a supplier that waits for the value of the callable
   */
  public static <T> Supplier<T> supplier(Callable<T> callable) {
    final var outcome = new OutcomeSupplier<T>();
    Thread.startVirtualThread(
        () -> {
          try {
            outcome.complete(callable.call(), null);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            outcome.complete(null, e);
          } catch (Throwable t) {
            outcome.complete(null, t);
          }
        });
    return outcome;
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
