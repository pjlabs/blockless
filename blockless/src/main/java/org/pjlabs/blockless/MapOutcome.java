package org.pjlabs.blockless;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Result of a {@link Parallel#tryAsMap} operation, holding both successes and failures keyed by
 * their input keys.
 *
 * <p>Both maps preserve the iteration order of the input keys.
 */
public record MapOutcome<K, V>(Map<K, V> successes, Map<K, Throwable> failures) {

  public MapOutcome {
    successes = Collections.unmodifiableMap(new LinkedHashMap<>(successes));
    failures = Collections.unmodifiableMap(new LinkedHashMap<>(failures));
  }

  /** Returns {@code true} if all tasks succeeded. */
  public boolean isComplete() {
    return failures.isEmpty();
  }
}
