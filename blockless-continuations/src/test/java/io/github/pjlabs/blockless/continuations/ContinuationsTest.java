package io.github.pjlabs.blockless.continuations;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.github.pjlabs.blockless.AsyncSupplier;
import io.github.pjlabs.blockless.Blockless;
import io.github.pjlabs.blockless.ContextPropagator;
import io.github.pjlabs.blockless.Parallel;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(15)
class ContinuationsTest {
  @Nested
  class WithoutBlockless {
    @Test
    void anOrdinarySupplierReadOccupiesThePlatformWorker() throws Exception {
      final var entered = new CountDownLatch(1);
      final var release = new CountDownLatch(1);
      try (final var executor = Executors.newSingleThreadExecutor()) {
        final var blocked =
            executor.submit(
                () ->
                    Blockless.get(
                        () -> {
                          entered.countDown();
                          release.await();
                          return "done";
                        }));
        try {
          assertTrue(entered.await(5, TimeUnit.SECONDS));
          final var sentinel = executor.submit(() -> "free");
          assertFalse(sentinel.isDone());
          release.countDown();
          assertEquals("done", blocked.get(5, TimeUnit.SECONDS));
          assertEquals("free", sentinel.get(5, TimeUnit.SECONDS));
        } finally {
          release.countDown();
        }
      }
    }
  }

  @Nested
  class WithBlockless {
    @Test
    void completedSupplierReturnsInlineWithoutSchedulingAResume() throws Exception {
      final var supplier = Blockless.supplier(() -> (String) null);
      assertNull(supplier.get());
      final var submissions = new AtomicInteger();
      Continuations.run(
              command -> {
                submissions.incrementAndGet();
                command.run();
              },
              new ContinuationTask() {
                @Override
                public void run() {
                  assertNull(supplier.get());
                  assertNull(supplier.get());
                }
              })
          .toCompletableFuture()
          .get(5, TimeUnit.SECONDS);
      assertEquals(1, submissions.get());
    }

    @Test
    void unsupportedSupplierIsRejectedBeforeItsPotentiallyBlockingBodyRuns() {
      final Supplier<String> arbitrary =
          () -> {
            fail("must not invoke an arbitrary supplier on the platform thread");
            return "unreachable";
          };
      final var completion =
          Continuations.run(
              Runnable::run,
              new ContinuationTask() {
                @Override
                public void run() {
                  arbitrary.get();
                }
              });
      assertInstanceOf(
          IllegalArgumentException.class,
          assertThrows(
                  ExecutionException.class,
                  () -> completion.toCompletableFuture().get(5, TimeUnit.SECONDS))
              .getCause());
    }

    @Test
    void resumesAfterRealSocketIoWithoutOccupyingThePlatformWorker() throws Exception {
      final var result = new AtomicReference<String>();
      try (final var listener = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
          final var executor = Executors.newSingleThreadExecutor()) {
        listener.setSoTimeout(5_000);
        final var completion =
            Continuations.run(
                executor,
                new ContinuationTask() {
                  @Override
                  public void run() {
                    final var response =
                        Blockless.supplier(
                                () -> {
                                  assertTrue(Thread.currentThread().isVirtual());
                                  try (final var socket =
                                      new Socket(
                                          listener.getInetAddress(), listener.getLocalPort())) {
                                    socket.setSoTimeout(5_000);
                                    return new DataInputStream(socket.getInputStream()).readUTF();
                                  }
                                })
                            .get();
                    result.set("received: " + response);
                  }
                });
        try (final var connection = listener.accept()) {
          assertEquals("free", executor.submit(() -> "free").get(5, TimeUnit.SECONDS));
          assertNull(result.get());
          final var output = new DataOutputStream(connection.getOutputStream());
          output.writeUTF("ready");
          output.flush();
          completion.toCompletableFuture().get(5, TimeUnit.SECONDS);
          assertEquals("received: ready", result.get());
        }
      }
    }

    @Test
    void releasesTheOnlyPlatformWorkerAndResumesWithoutRepeatingSideEffects() throws Exception {
      final var entered = new CountDownLatch(1);
      final var release = new CountDownLatch(1);
      final var starts = new AtomicInteger();
      final var result = new AtomicReference<String>();
      final var worker = new AtomicReference<Thread>();
      try (final var executor = Executors.newSingleThreadExecutor()) {
        final var completion =
            Continuations.run(
                executor,
                new ContinuationTask() {
                  @Override
                  public void run() {
                    assertFalse(Thread.currentThread().isVirtual());
                    worker.set(Thread.currentThread());
                    starts.incrementAndGet();
                    final var prefix = "saved local: ";
                    final long number = 42L;
                    final double fraction = 0.5;
                    final var supplier =
                        Blockless.supplier(
                            () -> {
                              assertTrue(Thread.currentThread().isVirtual());
                              entered.countDown();
                              release.await();
                              return "ready";
                            });
                    final var value = supplier.get();
                    assertSame(worker.get(), Thread.currentThread());
                    result.set(prefix + number + fraction + value);
                  }
                });
        try {
          assertTrue(entered.await(5, TimeUnit.SECONDS));
          assertEquals(
              worker.get(), executor.submit(Thread::currentThread).get(5, TimeUnit.SECONDS));
          assertNull(result.get());
          assertFalse(completion.toCompletableFuture().isDone());
          release.countDown();
          completion.toCompletableFuture().get(5, TimeUnit.SECONDS);
          assertEquals("saved local: 420.5ready", result.get());
          assertEquals(1, starts.get());
        } finally {
          release.countDown();
        }
      }
    }

    @Test
    void preservesNestedFramesAndFinallyUntilTheResultIsReady() throws Exception {
      final var stage = new CompletableFuture<String>();
      final var result = new AtomicReference<String>();
      final var cleanups = new AtomicInteger();
      try (final var executor = Executors.newSingleThreadExecutor()) {
        final var completion =
            Continuations.run(
                executor,
                new ContinuationTask() {
                  @Override
                  public void run() {
                    try {
                      result.set(nested(Blockless.supplier(stage), 7L));
                    } finally {
                      cleanups.incrementAndGet();
                    }
                  }
                });
        executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
        assertEquals(0, cleanups.get());
        stage.complete("value");
        completion.toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertEquals("nested7value", result.get());
        assertEquals(1, cleanups.get());
      }
    }

    @Test
    void failureIsThrownAtTheSuspensionPointAndFinallyRunsOnce() throws Exception {
      final var stage = new CompletableFuture<String>();
      final var failure = new IllegalArgumentException("failed later");
      final var caught = new AtomicReference<Throwable>();
      final var cleanups = new AtomicInteger();
      try (final var executor = Executors.newSingleThreadExecutor()) {
        final var completion =
            Continuations.run(
                executor,
                new ContinuationTask() {
                  @Override
                  public void run() {
                    try {
                      Blockless.supplier(stage).get();
                      fail("must throw");
                    } catch (IllegalArgumentException expected) {
                      caught.set(expected);
                    } finally {
                      cleanups.incrementAndGet();
                    }
                  }
                });
        executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
        stage.completeExceptionally(failure);
        completion.toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertSame(failure, caught.get());
        assertEquals(1, cleanups.get());
      }
    }

    @Test
    void parallelAsyncStartsBothTasksBeforeReadingAndDoesNotOccupyThePlatformWorker()
        throws Exception {
      final var entered = new CountDownLatch(2);
      final var release = new CountDownLatch(1);
      final var results = new AtomicReference<List<String>>();
      try (final var executor = Executors.newSingleThreadExecutor()) {
        final var completion =
            Continuations.run(
                executor,
                new ContinuationTask() {
                  @Override
                  public void run() {
                    final var parallel = Parallel.create();
                    final var first = parallel.async(() -> gated(entered, release, "first"));
                    final var second = parallel.async(() -> gated(entered, release, "second"));
                    results.set(List.of(first.get(), second.get()));
                  }
                });
        try {
          assertTrue(entered.await(5, TimeUnit.SECONDS));
          assertEquals("free", executor.submit(() -> "free").get(5, TimeUnit.SECONDS));
          release.countDown();
          completion.toCompletableFuture().get(5, TimeUnit.SECONDS);
          assertEquals(List.of("first", "second"), results.get());
        } finally {
          release.countDown();
        }
      }
    }

    @Test
    void directGetPreservesCheckedFailureAndErrors() throws Exception {
      for (final var failure : List.of(new IOException("checked"), new AssertionError("error"))) {
        try (final var executor = Executors.newSingleThreadExecutor()) {
          final var completion =
              Continuations.run(
                  executor,
                  new ContinuationTask() {
                    @Override
                    public void run() {
                      Blockless.get(
                          () -> {
                            if (failure instanceof Error error) {
                              throw error;
                            }
                            throw (IOException) failure;
                          });
                    }
                  });
          final var thrown =
              assertThrows(
                  ExecutionException.class,
                  () -> completion.toCompletableFuture().get(5, TimeUnit.SECONDS));
          assertSame(
              failure, failure instanceof Error ? thrown.getCause() : thrown.getCause().getCause());
        }
      }
    }

    @Test
    void completionDuringRegistrationTrampolinesInsteadOfRecursivelyResuming() throws Exception {
      final var reads = new AtomicInteger();
      final Supplier<Integer> readyDuringRegistration =
          new AsyncSupplier<>() {
            @Override
            public boolean isDone() {
              return false;
            }

            @Override
            public void onComplete(Runnable action) {
              action.run();
            }

            @Override
            public Integer get() {
              return reads.incrementAndGet();
            }
          };
      Continuations.run(
              Runnable::run,
              new ContinuationTask() {
                @Override
                public void run() {
                  for (int i = 0; i < 20_000; i++) {
                    assertEquals(i + 1, readyDuringRegistration.get());
                  }
                }
              })
          .toCompletableFuture()
          .get(5, TimeUnit.SECONDS);
      assertEquals(20_000, reads.get());
    }

    @Test
    void virtualThreadCallerWaitsNormally() throws Exception {
      final var result = new AtomicReference<Boolean>();
      try (final var executor = Executors.newVirtualThreadPerTaskExecutor()) {
        Continuations.run(
                executor,
                new ContinuationTask() {
                  @Override
                  public void run() {
                    final var value =
                        Blockless.get(
                            () -> {
                              Thread.sleep(10);
                              return Thread.currentThread().isVirtual();
                            });
                    result.set(value && Thread.currentThread().isVirtual());
                  }
                })
            .toCompletableFuture()
            .get(5, TimeUnit.SECONDS);
      }
      assertEquals(true, result.get());
    }

    @Test
    void propagatesAndRestoresContextOnEveryResume() throws Exception {
      final var context = new ThreadLocal<String>();
      final var propagator =
          new ContextPropagator() {
            @Override
            public Object capture() {
              return context.get();
            }

            @Override
            public Object attach(Object captured) {
              final var previous = context.get();
              context.set((String) captured);
              return previous;
            }

            @Override
            public void restore(Object previous) {
              if (previous == null) context.remove();
              else context.set((String) previous);
            }
          };
      final var stage = new CompletableFuture<String>();
      try (final var executor = Executors.newSingleThreadExecutor()) {
        executor.submit(() -> context.set("executor")).get(5, TimeUnit.SECONDS);
        context.set("request");
        final var completion =
            Continuations.run(
                executor,
                new ContinuationTask() {
                  @Override
                  public void run() {
                    assertEquals("request", context.get());
                    final var value = Blockless.supplier(stage).get();
                    assertEquals("ready", value);
                    assertEquals("request", context.get());
                  }
                },
                propagator);
        assertEquals("executor", executor.submit(context::get).get(5, TimeUnit.SECONDS));
        stage.complete("ready");
        completion.toCompletableFuture().get(5, TimeUnit.SECONDS);
        assertEquals("executor", executor.submit(context::get).get(5, TimeUnit.SECONDS));
        assertEquals("request", context.get());
      } finally {
        context.remove();
      }
    }

    @Test
    void rejectedSubmissionIsReported() {
      final var completion =
          Continuations.run(
              command -> {
                throw new RejectedExecutionException("closed");
              },
              new ContinuationTask() {
                @Override
                public void run() {
                  fail("must not execute");
                }
              });
      assertInstanceOf(
          RejectedExecutionException.class,
          assertThrows(
                  ExecutionException.class,
                  () -> completion.toCompletableFuture().get(5, TimeUnit.SECONDS))
              .getCause());
    }
  }

  @Suspendable
  private static String nested(Supplier<String> supplier, long value) {
    final var prefix = "nested" + value;
    return prefix + supplier.get();
  }

  private static String gated(CountDownLatch entered, CountDownLatch release, String value) {
    assertTrue(Thread.currentThread().isVirtual());
    entered.countDown();
    try {
      release.await();
      return value;
    } catch (InterruptedException failure) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(failure);
    }
  }
}
