package io.github.pjlabs.blockless;

import java.util.Objects;

/**
 * A value that is either a successful {@link #result()} or a failing {@link #failure()}, never
 * both.
 */
public record Either<Result, Failure>(Result result, Failure failure) {

  public Either {
    if (result == null && failure == null) {
      throw new IllegalArgumentException("Either must have a result or a failure");
    }
    if (result != null && failure != null) {
      throw new IllegalArgumentException("Either cannot have both a result and a failure");
    }
  }

  /** Success: {@code result} is set, {@code failure} is {@code null}. */
  public static <R, F> Either<R, F> ok(R result) {
    return new Either<>(Objects.requireNonNull(result, "result"), null);
  }

  /** Failure: {@code failure} is set, {@code result} is {@code null}. */
  public static <R, F> Either<R, F> fail(F failure) {
    return new Either<>(null, Objects.requireNonNull(failure, "failure"));
  }

  /** {@code true} when this instance holds a successful result. */
  public boolean isOk() {
    return failure == null;
  }

  /** {@code true} when this instance holds a failure. */
  public boolean isFailed() {
    return result == null;
  }
}
