package org.pjlabs.blockless;

import java.util.List;

/**
 * Result of a {@link Parallel#tryMap} operation, holding both successes and failures.
 *
 * <p>Successes preserve the relative order of successful items from the input list. Failures
 * contain the original exceptions (unwrapped from {@link RuntimeException}).
 */
public record Outcome<T>(List<T> successes, List<Throwable> failures) {

  public Outcome {
    successes = List.copyOf(successes);
    failures = List.copyOf(failures);
  }

  /** Returns {@code true} if all tasks succeeded. */
  public boolean isComplete() {
    return failures.isEmpty();
  }
}
