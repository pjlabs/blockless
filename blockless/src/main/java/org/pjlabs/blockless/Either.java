package org.pjlabs.blockless;

import java.util.Objects;

/**
 * A value that is either a successful {@link #response()} or a failing {@link #error()}, never
 * both.
 */
public record Either<Response, Error>(Response response, Error error) {

  public Either {
    if (response == null && error == null) {
      throw new IllegalArgumentException("Either must have a response or an error");
    }
    if (response != null && error != null) {
      throw new IllegalArgumentException("Either cannot have both a response and an error");
    }
  }

  /** Success: {@code response} is set, {@code error} is {@code null}. */
  public static <R, E> Either<R, E> ok(R response) {
    return new Either<>(Objects.requireNonNull(response, "response"), null);
  }

  /** Failure: {@code error} is set, {@code response} is {@code null}. */
  public static <R, E> Either<R, E> err(E error) {
    return new Either<>(null, Objects.requireNonNull(error, "error"));
  }

  /** {@code true} when this instance holds a response (success). */
  public boolean isOk() {
    return error == null;
  }

  /** {@code true} when this instance holds an error (failure). */
  public boolean isErr() {
    return response == null;
  }
}
