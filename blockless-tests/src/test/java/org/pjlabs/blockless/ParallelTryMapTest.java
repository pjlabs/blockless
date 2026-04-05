package org.pjlabs.blockless;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.pjlabs.blockless.context.slf4j.Slf4jMdcContextPropagator;
import org.slf4j.MDC;

class ParallelTryMapTest {

  private Parallel parallel;

  @BeforeEach
  void setUp() {
    MDC.clear();
    parallel = Parallel.create(new Slf4jMdcContextPropagator());
  }

  @AfterEach
  void tearDown() {
    MDC.clear();
  }

  @Nested
  class TryMap {

    @Test
    void returnsAllSuccessesWhenNoFailures() {
      final var outcome = parallel.tryMap(List.of(1, 2, 3), i -> i * 10);

      assertEquals(List.of(10, 20, 30), outcome.successes());
      assertTrue(outcome.failures().isEmpty());
      assertTrue(outcome.isComplete());
    }

    @Test
    void collectsPartialResultsOnFailure() {
      final var outcome =
          parallel.tryMap(
              List.of(1, 2, 3),
              i -> {
                if (i == 2) {
                  throw new IllegalArgumentException("bad: " + i);
                }
                return i * 10;
              });

      assertEquals(List.of(10, 30), outcome.successes());
      assertEquals(1, outcome.failures().size());
      assertFalse(outcome.isComplete());
    }

    @Test
    void collectsAllFailures() {
      final var outcome =
          parallel.tryMap(
              List.of(1, 2, 3),
              i -> {
                throw new RuntimeException("fail: " + i);
              });

      assertTrue(outcome.successes().isEmpty());
      assertEquals(3, outcome.failures().size());
      assertFalse(outcome.isComplete());
    }

    @Test
    void preservesOriginalException() {
      final var outcome =
          parallel.tryMap(
              List.of(1),
              i -> {
                throw new IllegalStateException("original");
              });

      assertEquals(1, outcome.failures().size());
      assertInstanceOf(IllegalStateException.class, outcome.failures().get(0));
      assertEquals("original", outcome.failures().get(0).getMessage());
    }

    @Test
    void runsConcurrently() {
      final var maxConcurrent = new AtomicInteger(0);
      final var current = new AtomicInteger(0);

      parallel.tryMap(
          List.of(1, 2, 3, 4, 5),
          i -> {
            final int c = current.incrementAndGet();
            maxConcurrent.updateAndGet(max -> Math.max(max, c));
            try {
              Thread.sleep(50);
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            current.decrementAndGet();
            return i;
          });

      assertTrue(
          maxConcurrent.get() > 1,
          "expected concurrent execution, but max concurrency was " + maxConcurrent.get());
    }

    @Test
    void propagatesMdc() {
      MDC.put("traceId", "trace-try");

      final var outcome = parallel.tryMap(List.of(1, 2, 3), i -> MDC.get("traceId"));

      assertTrue(
          outcome.successes().stream().allMatch("trace-try"::equals),
          "all tasks must see the propagated MDC value");
    }
  }

  @Nested
  class TryAsMap {

    @Test
    void associatesFailuresWithKeys() {
      final var outcome =
          parallel.tryAsMap(
              List.of("good", "bad"),
              key -> {
                if ("bad".equals(key)) {
                  throw new IllegalArgumentException("bad key");
                }
                return key.toUpperCase();
              });

      assertEquals("GOOD", outcome.successes().get("good"));
      assertInstanceOf(IllegalArgumentException.class, outcome.failures().get("bad"));
      assertFalse(outcome.isComplete());
    }

    @Test
    void preservesKeyOrder() {
      final var outcome = parallel.tryAsMap(List.of("c", "a", "b"), String::toUpperCase);

      assertEquals(List.of("c", "a", "b"), List.copyOf(outcome.successes().keySet()));
      assertTrue(outcome.isComplete());
    }
  }

  @Nested
  class ExistingBehavior {

    @Test
    void mapStillFailsFast() {
      assertThrows(
          RuntimeException.class,
          () ->
              parallel.map(
                  List.of(1, 2, 3),
                  i -> {
                    if (i == 2) {
                      throw new IllegalArgumentException("fail");
                    }
                    return i;
                  }));
    }
  }
}
