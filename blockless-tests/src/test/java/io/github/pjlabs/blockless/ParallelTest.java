package io.github.pjlabs.blockless;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.pjlabs.blockless.context.slf4j.Slf4jMdcContextPropagator;
import java.nio.file.Files;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordingFile;
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
  void asyncStartsOnlyTheWorkerThread() throws Exception {
    final var workerIds = new HashSet<Long>();
    final var startedIds =
        startedVirtualThreadIds(
            () -> {
              for (final var p : List.of(parallel, parallel.withTimeout(Duration.ofSeconds(5)))) {
                for (int i = 0; i < 50; i++) {
                  workerIds.add(p.async(() -> Thread.currentThread().threadId()).get());
                }
              }
            });

    assertEquals(100, workerIds.size());
    assertEquals(
        workerIds, startedIds, "async() must not start virtual threads besides its worker");
  }

  private static Set<Long> startedVirtualThreadIds(Runnable action) throws Exception {
    final var file = Files.createTempFile("virtual-thread-starts", ".jfr");
    try (final var recording = new Recording()) {
      recording.enable("jdk.VirtualThreadStart");
      recording.start();
      action.run();
      recording.stop();
      recording.dump(file);
      return RecordingFile.readAllEvents(file).stream()
          .filter(event -> event.getEventType().getName().equals("jdk.VirtualThreadStart"))
          .map(event -> event.getLong("javaThreadId"))
          .collect(Collectors.toSet());
    } finally {
      Files.deleteIfExists(file);
    }
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
  void mapAllowsNullResults() {
    final var results = parallel.map(List.of(1, 2, 3), i -> i == 2 ? null : i);
    assertEquals(3, results.size());
    assertEquals(1, results.get(0));
    assertNull(results.get(1));
    assertEquals(3, results.get(2));
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
  class SlidingWindow {

    @Test
    void limitsAliveVirtualThreads() {
      final var boundedParallel =
          Parallel.create(new Slf4jMdcContextPropagator()).withMaxConcurrency(3);
      final var maxAlive = new AtomicInteger(0);
      final var alive = new AtomicInteger(0);
      final var items = IntStream.rangeClosed(1, 100).boxed().toList();

      boundedParallel.map(
          items,
          i -> {
            final int a = alive.incrementAndGet();
            maxAlive.updateAndGet(max -> Math.max(max, a));
            try {
              Thread.sleep(10);
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            alive.decrementAndGet();
            return i;
          });

      assertTrue(maxAlive.get() <= 3, "expected at most 3 VTs alive, but was " + maxAlive.get());
      assertTrue(
          maxAlive.get() >= 2,
          "expected concurrent execution, but max alive was " + maxAlive.get());
    }

    @Test
    void drainsCompletedTasksOpportunistically() {
      final var boundedParallel =
          Parallel.create(new Slf4jMdcContextPropagator()).withMaxConcurrency(3);
      // First task slow, rest fast — drain should free slots without waiting on each individually
      // 9 items with window=3: without drain, slow task blocks 2 slots idle
      // With drain, fast tasks behind the slow one are collected immediately after the slow one
      final var items = List.of(200, 10, 10, 10, 10, 10, 10, 10, 10);

      final var start = System.nanoTime();
      final var results =
          boundedParallel.map(
              items,
              i -> {
                try {
                  Thread.sleep(i);
                } catch (final InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
                return i;
              });
      final var elapsedMs = (System.nanoTime() - start) / 1_000_000;

      assertEquals(items, results);
      // With window=3 and drain: ~200ms (slow task) + ~30ms (remaining 6 fast tasks in 2 batches)
      // Without drain: ~200ms + ~40ms (each slot freed one-at-a-time)
      // Either way should complete well under 1s
      assertTrue(elapsedMs < 1000, "expected under 1s, but took " + elapsedMs + "ms");
    }

    @Test
    void unboundedLaunchesAllEagerly() {
      final var maxAlive = new AtomicInteger(0);
      final var alive = new AtomicInteger(0);
      final var items = IntStream.rangeClosed(1, 20).boxed().toList();

      parallel.map(
          items,
          i -> {
            final int a = alive.incrementAndGet();
            maxAlive.updateAndGet(max -> Math.max(max, a));
            try {
              Thread.sleep(50);
            } catch (final InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            alive.decrementAndGet();
            return i;
          });

      assertTrue(
          maxAlive.get() > 3,
          "expected many VTs alive without maxConcurrency, but was " + maxAlive.get());
    }
  }

  @Nested
  class Cancellation {

    @Test
    void cancelsRemainingTasksOnFailure() {
      final var boundedParallel =
          Parallel.create(new Slf4jMdcContextPropagator()).withMaxConcurrency(2);
      final var completed = new AtomicInteger(0);
      final var started = new AtomicInteger(0);

      assertThrows(
          RuntimeException.class,
          () ->
              boundedParallel.map(
                  IntStream.rangeClosed(1, 20).boxed().toList(),
                  i -> {
                    started.incrementAndGet();
                    if (i == 1) {
                      throw new IllegalArgumentException("fail on first");
                    }
                    try {
                      Thread.sleep(500);
                    } catch (final InterruptedException e) {
                      Thread.currentThread().interrupt();
                      throw new RuntimeException(e);
                    }
                    completed.incrementAndGet();
                    return i;
                  }));

      assertTrue(
          started.get() < 20,
          "expected sliding window to prevent launching all 20, but started " + started.get());
    }

    @Test
    void toEitherDoesNotCancelOnFailure() {
      final var boundedParallel =
          Parallel.create(new Slf4jMdcContextPropagator()).withMaxConcurrency(3);
      final var completed = new AtomicInteger(0);

      final var eithers =
          boundedParallel.toEither(
              IntStream.rangeClosed(1, 10).boxed().toList(),
              i -> {
                if (i == 3) {
                  throw new IllegalArgumentException("fail on 3");
                }
                completed.incrementAndGet();
                return i * 10;
              });

      assertEquals(10, eithers.size());
      assertEquals(9, completed.get(), "all non-failing tasks must complete");
      assertTrue(eithers.get(2).isFailed());
      assertEquals(9, eithers.stream().filter(Either::isOk).count());
    }
  }

  @Nested
  class Throughput {

    @Test
    void handlesHighItemCountWithSlidingWindow() {
      final var boundedParallel =
          Parallel.create(new Slf4jMdcContextPropagator()).withMaxConcurrency(50);
      final var items = IntStream.rangeClosed(1, 10_000).boxed().toList();
      final var maxAlive = new AtomicInteger(0);
      final var alive = new AtomicInteger(0);

      final var start = System.nanoTime();
      final var results =
          boundedParallel.map(
              items,
              i -> {
                final int a = alive.incrementAndGet();
                maxAlive.updateAndGet(max -> Math.max(max, a));
                alive.decrementAndGet();
                return i * 2;
              });
      final var elapsedMs = (System.nanoTime() - start) / 1_000_000;

      assertEquals(10_000, results.size());
      assertEquals(2, results.get(0));
      assertEquals(20_000, results.get(9_999));
      assertTrue(maxAlive.get() <= 50, "expected at most 50 VTs alive, but was " + maxAlive.get());
      assertTrue(elapsedMs < 10_000, "expected completion under 10s, but took " + elapsedMs + "ms");
    }

    @Test
    void handlesHighItemCountUnbounded() {
      final var items = IntStream.rangeClosed(1, 10_000).boxed().toList();

      final var start = System.nanoTime();
      final var results = parallel.map(items, i -> i * 2);
      final var elapsedMs = (System.nanoTime() - start) / 1_000_000;

      assertEquals(10_000, results.size());
      assertEquals(2, results.get(0));
      assertEquals(20_000, results.get(9_999));
      assertTrue(elapsedMs < 10_000, "expected completion under 10s, but took " + elapsedMs + "ms");
    }

    @Test
    void toEitherHandlesHighItemCount() {
      final var boundedParallel =
          Parallel.create(new Slf4jMdcContextPropagator()).withMaxConcurrency(50);
      final var items = IntStream.rangeClosed(1, 10_000).boxed().toList();

      final var start = System.nanoTime();
      final var results =
          boundedParallel.toEither(
              items,
              i -> {
                if (i % 1000 == 0) {
                  throw new RuntimeException("fail on " + i);
                }
                return i * 2;
              });
      final var elapsedMs = (System.nanoTime() - start) / 1_000_000;

      assertEquals(10_000, results.size());
      assertEquals(10, results.stream().filter(Either::isFailed).count());
      assertEquals(9_990, results.stream().filter(Either::isOk).count());
      assertTrue(elapsedMs < 10_000, "expected completion under 10s, but took " + elapsedMs + "ms");
    }

    @Test
    void propagatesMdcUnderHighItemCount() {
      MDC.put("traceId", "high-throughput-trace");
      final var boundedParallel =
          Parallel.create(new Slf4jMdcContextPropagator()).withMaxConcurrency(50);
      final var items = IntStream.rangeClosed(1, 1_000).boxed().toList();

      final var results = boundedParallel.map(items, i -> MDC.get("traceId"));

      assertTrue(
          results.stream().allMatch("high-throughput-trace"::equals),
          "all 1000 tasks must see propagated MDC");
    }
  }

  @Nested
  class Timeout {

    @Test
    void interruptingAsyncWaiterInterruptsWorker() throws Exception {
      final var started = new CountDownLatch(1);
      final var interrupted = new CountDownLatch(1);
      final var release = new CountDownLatch(1);
      final var finished = new CountDownLatch(1);
      final var supplier =
          parallel
              .withTimeout(Duration.ofSeconds(30))
              .async(
                  () -> {
                    started.countDown();
                    try {
                      release.await();
                      return "done";
                    } catch (InterruptedException e) {
                      interrupted.countDown();
                      Thread.currentThread().interrupt();
                      throw new RuntimeException(e);
                    } finally {
                      finished.countDown();
                    }
                  });
      final var waiting = new FutureTask<>(supplier::get);
      final var caller = Thread.startVirtualThread(waiting);
      try {
        assertTrue(started.await(3, TimeUnit.SECONDS));
        caller.interrupt();
        final var failure =
            assertThrows(ExecutionException.class, () -> waiting.get(3, TimeUnit.SECONDS));
        assertInstanceOf(InterruptedException.class, failure.getCause().getCause());
        assertTrue(interrupted.await(3, TimeUnit.SECONDS));
      } finally {
        release.countDown();
        caller.interrupt();
        caller.join(3000);
        assertFalse(caller.isAlive());
        assertTrue(finished.await(3, TimeUnit.SECONDS));
      }
    }

    @Test
    void mapFailureInterruptsSiblingWorkerWithTimeoutEnabled() throws InterruptedException {
      final var siblingStarted = new CountDownLatch(1);
      final var siblingInterrupted = new CountDownLatch(1);
      final var release = new CountDownLatch(1);
      final var siblingFinished = new CountDownLatch(1);
      final var original = new IllegalArgumentException("failed");
      try {
        final var failure =
            assertTimeoutPreemptively(
                Duration.ofSeconds(3),
                () ->
                    assertThrows(
                        IllegalArgumentException.class,
                        () ->
                            parallel
                                .withMaxConcurrency(2)
                                .withTimeout(Duration.ofSeconds(30))
                                .map(
                                    List.of(1, 2),
                                    i -> {
                                      if (i == 1) {
                                        try {
                                          if (!siblingStarted.await(3, TimeUnit.SECONDS)) {
                                            throw new IllegalStateException(
                                                "sibling did not start");
                                          }
                                        } catch (InterruptedException e) {
                                          throw new RuntimeException(e);
                                        }
                                        throw original;
                                      }
                                      siblingStarted.countDown();
                                      try {
                                        release.await();
                                        return i;
                                      } catch (InterruptedException e) {
                                        siblingInterrupted.countDown();
                                        Thread.currentThread().interrupt();
                                        throw new RuntimeException(e);
                                      } finally {
                                        siblingFinished.countDown();
                                      }
                                    })));
        assertSame(original, failure);
        assertTrue(
            siblingInterrupted.await(3, TimeUnit.SECONDS),
            "map failure must interrupt the actual sibling worker, not just a waiter");
      } finally {
        release.countDown();
        assertTrue(siblingFinished.await(3, TimeUnit.SECONDS));
      }
    }

    @Test
    void timedOutWorkerRetainsSlotUntilItExits() throws Exception {
      final var release = new CountDownLatch(1);
      final var interrupted = new CountDownLatch(1);
      final var nextStarted = new CountDownLatch(1);
      final var active = new AtomicInteger();
      final var maxActive = new AtomicInteger();
      final var batch =
          new FutureTask<>(
              () ->
                  parallel
                      .withMaxConcurrency(1)
                      .withTimeout(Duration.ofMillis(100))
                      .toEither(
                          List.of(1, 2),
                          i -> {
                            maxActive.accumulateAndGet(active.incrementAndGet(), Math::max);
                            try {
                              if (i == 1) {
                                return waitIgnoringInterrupts(release, interrupted);
                              }
                              nextStarted.countDown();
                              return "next";
                            } finally {
                              active.decrementAndGet();
                            }
                          }));
      final var caller = Thread.startVirtualThread(batch);
      try {
        assertTrue(interrupted.await(3, TimeUnit.SECONDS));
        assertFalse(
            nextStarted.await(200, TimeUnit.MILLISECONDS),
            "timeout must not free the worker's concurrency slot");
        assertFalse(batch.isDone(), "queued items still need a worker slot");
        release.countDown();

        final var results = batch.get(3, TimeUnit.SECONDS);
        assertInstanceOf(TimeoutException.class, results.get(0).failure());
        assertEquals("next", results.get(1).result());
        assertEquals(1, maxActive.get());
      } finally {
        release.countDown();
        caller.interrupt();
        caller.join(3000);
        assertFalse(caller.isAlive());
      }
    }

    @Test
    void timedAsyncCapturesAndRestoresContextOnce() {
      final var captures = new AtomicInteger();
      final var attaches = new AtomicInteger();
      final var restores = new AtomicInteger();
      final var propagator =
          new ContextPropagator() {
            @Override
            public Object capture() {
              captures.incrementAndGet();
              return null;
            }

            @Override
            public Object attach(Object captured) {
              attaches.incrementAndGet();
              return null;
            }

            @Override
            public void restore(Object previous) {
              restores.incrementAndGet();
            }
          };

      assertEquals(
          "ok",
          Parallel.create(propagator).withTimeout(Duration.ofSeconds(5)).async(() -> "ok").get());
      assertEquals(1, captures.get());
      assertEquals(1, attaches.get());
      assertEquals(1, restores.get());
    }

    @Test
    void asyncTimesOutEvenWhenTaskIgnoresInterruption() throws InterruptedException {
      final var release = new CountDownLatch(1);
      final var interrupted = new CountDownLatch(1);
      final var finished = new CountDownLatch(1);
      final var supplier =
          parallel
              .withTimeout(Duration.ofMillis(100))
              .async(
                  () -> {
                    try {
                      return waitIgnoringInterrupts(release, interrupted);
                    } finally {
                      finished.countDown();
                    }
                  });

      try {
        final var failure =
            assertTimeoutPreemptively(
                Duration.ofSeconds(3), () -> assertThrows(RuntimeException.class, supplier::get));
        assertInstanceOf(TimeoutException.class, failure.getCause());
        assertTrue(interrupted.await(3, TimeUnit.SECONDS));
        assertEquals(1, finished.getCount(), "caller must return before the task finishes");
      } finally {
        release.countDown();
        assertTrue(finished.await(3, TimeUnit.SECONDS));
      }
    }

    @Test
    void mapTimesOutEvenWhenTaskIgnoresInterruption() {
      final var release = new CountDownLatch(1);
      try {
        final var failure =
            assertTimeoutPreemptively(
                Duration.ofSeconds(3),
                () ->
                    assertThrows(
                        RuntimeException.class,
                        () ->
                            parallel
                                .withTimeout(Duration.ofMillis(100))
                                .map(
                                    List.of(1),
                                    i -> waitIgnoringInterrupts(release, new CountDownLatch(1)))));
        assertInstanceOf(TimeoutException.class, failure.getCause());
      } finally {
        release.countDown();
      }
    }

    @Test
    void toEitherCollectsTimeoutAndSuccess() {
      final var release = new CountDownLatch(1);
      try {
        final var results =
            assertTimeoutPreemptively(
                Duration.ofSeconds(3),
                () ->
                    parallel
                        .withTimeout(Duration.ofMillis(100))
                        .toEither(
                            List.of(1, 2),
                            i ->
                                i == 1
                                    ? waitIgnoringInterrupts(release, new CountDownLatch(1))
                                    : "ok"));
        assertInstanceOf(TimeoutException.class, results.get(0).failure());
        assertEquals("ok", results.get(1).result());
      } finally {
        release.countDown();
      }
    }

    @Test
    void asyncPreservesContextAndRunsOnVirtualThread() {
      MDC.put("traceId", "timed-parent");
      final var supplier =
          parallel
              .withTimeout(Duration.ofSeconds(5))
              .async(
                  () -> {
                    assertTrue(Thread.currentThread().isVirtual());
                    final var traceId = MDC.get("traceId");
                    MDC.put("traceId", "timed-child");
                    return traceId;
                  });

      assertEquals("timed-parent", supplier.get());
      assertEquals("timed-parent", MDC.get("traceId"));
    }

    @Test
    void preservesTaskFailureBeforeTimeout() {
      final var original = new IllegalArgumentException("failed");
      final var supplier =
          parallel
              .withTimeout(Duration.ofSeconds(5))
              .async(
                  () -> {
                    throw original;
                  });
      assertSame(original, assertThrows(IllegalArgumentException.class, supplier::get));
    }

    private String waitIgnoringInterrupts(CountDownLatch release, CountDownLatch interrupted) {
      while (true) {
        try {
          release.await();
          return "late result";
        } catch (InterruptedException ignored) {
          interrupted.countDown();
        }
      }
    }

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
    void batchCanOutlastPerTaskTimeout() {
      final var timedParallel = parallel.withMaxConcurrency(1).withTimeout(Duration.ofMillis(100));
      final var items = IntStream.range(0, 10).boxed().toList();
      final var started = System.nanoTime();

      final var results =
          assertTimeoutPreemptively(
              Duration.ofSeconds(1),
              () ->
                  timedParallel.map(
                      items,
                      item -> {
                        try {
                          Thread.sleep(Duration.ofMillis(50));
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                          throw new RuntimeException(e);
                        }
                        return item;
                      }));

      assertEquals(items, results, "all ten tasks must finish within their individual timeout");
      assertTrue(
          System.nanoTime() - started >= Duration.ofMillis(500).toNanos(),
          "the sequential batch must be allowed to take 500 ms despite a 100 ms timeout");
    }

    @Test
    void mapFailsWithinOneSecondWhenTaskExceedsTimeout() throws InterruptedException {
      final var timedParallel = parallel.withMaxConcurrency(1).withTimeout(Duration.ofMillis(100));
      final var items = IntStream.range(0, 10).boxed().toList();
      final var interrupted = new CountDownLatch(1);

      final var failure =
          assertTimeoutPreemptively(
              Duration.ofSeconds(1),
              () ->
                  assertThrows(
                      RuntimeException.class,
                      () ->
                          timedParallel.map(
                              items,
                              item -> {
                                try {
                                  Thread.sleep(Duration.ofMillis(200));
                                } catch (InterruptedException e) {
                                  interrupted.countDown();
                                  Thread.currentThread().interrupt();
                                  throw new RuntimeException(e);
                                }
                                return item;
                              })));

      assertInstanceOf(TimeoutException.class, failure.getCause());
      assertTrue(interrupted.await(1, TimeUnit.SECONDS), "the timed-out task must be interrupted");
    }

    @Test
    void toEitherCollectsFiveTimeoutsWithinOneSecond() {
      final var timedParallel = parallel.withMaxConcurrency(1).withTimeout(Duration.ofMillis(100));
      final var items = IntStream.range(0, 5).boxed().toList();
      final var started = System.nanoTime();

      final var results =
          assertTimeoutPreemptively(
              Duration.ofSeconds(1),
              () ->
                  timedParallel.toEither(
                      items,
                      item -> {
                        try {
                          Thread.sleep(Duration.ofMillis(200));
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                          throw new RuntimeException(e);
                        }
                        return item;
                      }));

      assertEquals(5, results.size());
      results.forEach(result -> assertInstanceOf(TimeoutException.class, result.failure()));
      assertTrue(
          System.nanoTime() - started >= Duration.ofMillis(500).toNanos(),
          "five sequential task timeouts must take at least 500 ms");
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
