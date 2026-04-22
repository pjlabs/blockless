package io.github.pjlabs.blockless;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.pjlabs.blockless.context.slf4j.Slf4jMdcContextPropagator;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

class ParallelTest {

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

  @Test
  void mapReturnsResultsInInputOrder() {
    // Items with variable sleep times — results must match input order, not completion order
    final var items = List.of(3, 1, 2);
    final var results =
        parallel.map(
            items,
            i -> {
              try {
                Thread.sleep(i * 20L);
              } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              return i * 10;
            });

    assertEquals(List.of(30, 10, 20), results);
  }

  @Test
  void mapRunsConcurrently() {
    final var maxConcurrent = new AtomicInteger(0);
    final var current = new AtomicInteger(0);

    final var items = List.of(1, 2, 3, 4, 5);
    parallel.map(
        items,
        i -> {
          int c = current.incrementAndGet();
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
  void mapPropagatesMdc() {
    MDC.put("traceId", "trace-abc");

    final var results = parallel.map(List.of(1, 2, 3), i -> MDC.get("traceId"));

    assertTrue(
        results.stream().allMatch("trace-abc"::equals),
        "all tasks must see the propagated MDC value");
  }

  @Test
  void mapRunsOnVirtualThreads() {
    final var results = parallel.map(List.of(1, 2, 3), i -> Thread.currentThread().isVirtual());

    assertTrue(
        results.stream().allMatch(Boolean::booleanValue), "all tasks must run on virtual threads");
  }

  @Test
  void mapDoesNotAlterCallingThreadMdc() {
    MDC.put("traceId", "parent-value");

    parallel.map(
        List.of(1, 2, 3),
        i -> {
          // Tasks see propagated MDC
          assertEquals("parent-value", MDC.get("traceId"));
          return i;
        });

    assertEquals(
        "parent-value",
        MDC.get("traceId"),
        "calling thread MDC must be unchanged after map completes");
  }

  @Test
  void asyncPropagatesMdc() {
    MDC.put("traceId", "trace-xyz");

    final var supplier = parallel.async(() -> MDC.get("traceId"));
    assertEquals("trace-xyz", supplier.get());
  }

  @Test
  void asyncRunsOnVirtualThread() {
    final var supplier = parallel.async(() -> Thread.currentThread().isVirtual());
    assertTrue(supplier.get());
  }

  @Test
  void asMapPreservesKeyOrder() {
    final var keys = List.of("c", "a", "b");
    final var result = parallel.asMap(keys, k -> k.toUpperCase());

    assertEquals(
        List.of("c", "a", "b"), List.copyOf(result.keySet()), "keys must preserve iteration order");
    assertEquals("C", result.get("c"));
    assertEquals("A", result.get("a"));
    assertEquals("B", result.get("b"));
  }

  @Test
  void asMapPropagatesMdc() {
    MDC.put("traceId", "trace-map");

    final var result = parallel.asMap(List.of("x", "y"), k -> MDC.get("traceId"));

    assertTrue(
        result.values().stream().allMatch("trace-map"::equals),
        "all asMap tasks must see the propagated MDC value");
  }

  @Test
  void mapHandlesEmptyList() {
    assertEquals(List.of(), parallel.map(List.of(), i -> i));
  }

  @Test
  void asMapHandlesEmptyKeys() {
    assertEquals(0, parallel.asMap(List.of(), k -> k).size());
  }

  @Nested
  class BoundedConcurrency {

    @Test
    void limitsConcurrentTasks() {
      final var boundedParallel =
          Parallel.create(new Slf4jMdcContextPropagator()).withMaxConcurrency(2);
      final var maxConcurrent = new AtomicInteger(0);
      final var current = new AtomicInteger(0);

      boundedParallel.map(
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
          maxConcurrent.get() <= 2, "expected max 2 concurrent, but was " + maxConcurrent.get());
    }

    @Test
    void stillRunsConcurrently() {
      final var boundedParallel =
          Parallel.create(new Slf4jMdcContextPropagator()).withMaxConcurrency(3);
      final var maxConcurrent = new AtomicInteger(0);
      final var current = new AtomicInteger(0);

      boundedParallel.map(
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
    void preservesResultOrder() {
      final var boundedParallel =
          Parallel.create(new Slf4jMdcContextPropagator()).withMaxConcurrency(2);
      final var results = boundedParallel.map(List.of(3, 1, 2), i -> i * 10);
      assertEquals(List.of(30, 10, 20), results);
    }

    @Test
    void propagatesMdc() {
      MDC.put("traceId", "bounded-trace");
      final var boundedParallel =
          Parallel.create(new Slf4jMdcContextPropagator()).withMaxConcurrency(2);
      final var results = boundedParallel.map(List.of(1, 2, 3), i -> MDC.get("traceId"));
      assertTrue(results.stream().allMatch("bounded-trace"::equals));
    }

    @Test
    void rejectsZeroConcurrency() {
      assertThrows(
          IllegalArgumentException.class,
          () -> Parallel.create(new Slf4jMdcContextPropagator()).withMaxConcurrency(0));
    }
  }

  @Nested
  class Timeout {

    @Test
    void completesWithinTimeout() {
      final var timedParallel =
          Parallel.create(new Slf4jMdcContextPropagator()).withTimeout(Duration.ofSeconds(5));
      final var results = timedParallel.map(List.of(1, 2, 3), i -> i * 10);
      assertEquals(List.of(10, 20, 30), results);
    }

    @Test
    void interruptsSlowTask() {
      final var timedParallel =
          Parallel.create(new Slf4jMdcContextPropagator()).withTimeout(Duration.ofMillis(50));
      assertThrows(
          RuntimeException.class,
          () ->
              timedParallel.map(
                  List.of(1),
                  i -> {
                    try {
                      Thread.sleep(5000);
                    } catch (final InterruptedException e) {
                      Thread.currentThread().interrupt();
                      throw new RuntimeException(e);
                    }
                    return i;
                  }));
    }

    @Test
    void fastTasksUnaffected() {
      final var timedParallel =
          Parallel.create(new Slf4jMdcContextPropagator()).withTimeout(Duration.ofSeconds(1));
      final var result = timedParallel.map(List.of("a", "b"), s -> s.toUpperCase());
      assertEquals(List.of("A", "B"), result);
    }

    @Test
    void combinesWithMaxConcurrency() {
      final var timedParallel =
          Parallel.create(new Slf4jMdcContextPropagator())
              .withMaxConcurrency(2)
              .withTimeout(Duration.ofSeconds(5));
      final var results = timedParallel.map(List.of(1, 2, 3, 4), i -> i * 10);
      assertEquals(List.of(10, 20, 30, 40), results);
    }

    @Test
    void rejectsZeroTimeout() {
      assertThrows(
          IllegalArgumentException.class,
          () -> Parallel.create(new Slf4jMdcContextPropagator()).withTimeout(Duration.ZERO));
    }

    @Test
    void rejectsNegativeTimeout() {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              Parallel.create(new Slf4jMdcContextPropagator()).withTimeout(Duration.ofMillis(-1)));
    }
  }
}
