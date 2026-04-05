package org.pjlabs.blockless;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

class ParallelToEitherTest {

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
  class ToEither {

    @Test
    void returnsAllResponsesWhenNoFailures() {
      final var eithers = parallel.toEither(List.of(1, 2, 3), i -> i * 10);

      assertEquals(3, eithers.size());
      assertEquals(10, eithers.get(0).response());
      assertEquals(20, eithers.get(1).response());
      assertEquals(30, eithers.get(2).response());
      assertTrue(eithers.stream().allMatch(Either::isOk));
    }

    @Test
    void collectsPerIndexResultsOnFailure() {
      final var eithers =
          parallel.toEither(
              List.of(1, 2, 3),
              i -> {
                if (i == 2) {
                  throw new IllegalArgumentException("bad: " + i);
                }
                return i * 10;
              });

      assertEquals(3, eithers.size());
      assertEquals(10, eithers.get(0).response());
      assertTrue(eithers.get(1).isErr());
      assertInstanceOf(IllegalArgumentException.class, eithers.get(1).error());
      assertEquals(30, eithers.get(2).response());
    }

    @Test
    void collectsAllFailures() {
      final var eithers =
          parallel.toEither(
              List.of(1, 2, 3),
              i -> {
                throw new RuntimeException("fail: " + i);
              });

      assertEquals(3, eithers.size());
      assertTrue(eithers.stream().allMatch(Either::isErr));
    }

    @Test
    void preservesOriginalException() {
      final var eithers =
          parallel.toEither(
              List.of(1),
              i -> {
                throw new IllegalStateException("original");
              });

      assertEquals(1, eithers.size());
      assertTrue(eithers.get(0).isErr());
      assertInstanceOf(IllegalStateException.class, eithers.get(0).error());
      assertEquals("original", eithers.get(0).error().getMessage());
    }

    @Test
    void runsConcurrently() {
      final var maxConcurrent = new AtomicInteger(0);
      final var current = new AtomicInteger(0);

      parallel.toEither(
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

      final var eithers = parallel.toEither(List.of(1, 2, 3), i -> MDC.get("traceId"));

      assertTrue(
          eithers.stream().map(Either::response).allMatch("trace-try"::equals),
          "all tasks must see the propagated MDC value");
    }
  }

  @Nested
  class ToEitherMap {

    @Test
    void associatesErrorsWithKeys() {
      final var map =
          parallel.toEitherMap(
              List.of("good", "bad"),
              key -> {
                if ("bad".equals(key)) {
                  throw new IllegalArgumentException("bad key");
                }
                return key.toUpperCase();
              });

      assertEquals("GOOD", map.get("good").response());
      assertTrue(map.get("bad").isErr());
      assertInstanceOf(IllegalArgumentException.class, map.get("bad").error());
    }

    @Test
    void preservesKeyOrder() {
      final var map = parallel.toEitherMap(List.of("c", "a", "b"), String::toUpperCase);

      assertEquals(List.of("c", "a", "b"), List.copyOf(map.keySet()));
      assertTrue(map.values().stream().allMatch(Either::isOk));
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
