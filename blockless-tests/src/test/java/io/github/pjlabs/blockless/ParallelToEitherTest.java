package io.github.pjlabs.blockless;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.pjlabs.blockless.context.slf4j.Slf4jMdcContextPropagator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
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
      assertEquals(10, eithers.get(0).result());
      assertEquals(20, eithers.get(1).result());
      assertEquals(30, eithers.get(2).result());
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
      assertEquals(10, eithers.get(0).result());
      assertTrue(eithers.get(1).isFailed());
      assertInstanceOf(IllegalArgumentException.class, eithers.get(1).failure());
      assertEquals(30, eithers.get(2).result());
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
      assertTrue(eithers.stream().allMatch(Either::isFailed));
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
      assertTrue(eithers.get(0).isFailed());
      assertInstanceOf(IllegalStateException.class, eithers.get(0).failure());
      assertEquals("original", eithers.get(0).failure().getMessage());
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
          eithers.stream().map(Either::result).allMatch("trace-try"::equals),
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

      assertEquals("GOOD", map.get("good").result());
      assertTrue(map.get("bad").isFailed());
      assertInstanceOf(IllegalArgumentException.class, map.get("bad").failure());
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
