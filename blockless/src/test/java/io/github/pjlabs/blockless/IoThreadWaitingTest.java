package io.github.pjlabs.blockless;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Compares the two waits against the same single-threaded, real socket I/O loop. */
@Timeout(10)
class IoThreadWaitingTest {

  enum WaitStrategy {
    JOIN,
    LATCH;

    Supplier<String> supplier(CompletableFuture<String> stage) {
      return this == JOIN ? stage::join : Blockless.supplier(stage);
    }
  }

  @Nested
  class WithoutBlockless {
    @ParameterizedTest(name = "join, virtual caller={0}: I/O executor keeps processing")
    @ValueSource(booleans = {false, true})
    void joinOnRequestThread(boolean virtual) throws Exception {
      waitingCallerLeavesIoThreadFree(WaitStrategy.JOIN, virtual);
    }
  }

  @Nested
  class WithBlockless {
    @ParameterizedTest(
        name = "Blockless supplier, virtual caller={0}: I/O executor keeps processing")
    @ValueSource(booleans = {false, true})
    void supplierOnRequestThread(boolean virtual) throws Exception {
      waitingCallerLeavesIoThreadFree(WaitStrategy.LATCH, virtual);
    }
  }

  private void waitingCallerLeavesIoThreadFree(WaitStrategy strategy, boolean virtual)
      throws Exception {
    final var first = new CompletableFuture<String>();
    final var secondProcessedBy = new CompletableFuture<Thread>();
    final var supplier = strategy.supplier(first);
    try (final var io =
        new SocketEventLoop(
            message -> {
              if (message == 1) {
                first.complete("first response");
              } else {
                secondProcessedBy.complete(Thread.currentThread());
              }
            })) {
      final var result = new FutureTask<>(supplier::get);
      final Thread.Builder builder = virtual ? Thread.ofVirtual() : Thread.ofPlatform().daemon();
      final var caller = builder.name("request-caller").start(result);
      try {
        awaitParked(caller);
        // Deliver another response before the response this caller is waiting for.
        io.send(2);
        assertSame(io.thread, secondProcessedBy.get(2, TimeUnit.SECONDS));
        assertFalse(result.isDone(), "Request must still be waiting for the first response");
        assertFalse(io.thread.isVirtual(), "The selector loop runs on a platform thread");
        assertEquals(virtual, caller.isVirtual());
        io.send(1);
        assertEquals("first response", result.get(2, TimeUnit.SECONDS));
      } finally {
        first.completeExceptionally(new IllegalStateException("test finished"));
        caller.join(2000);
        assertFalse(caller.isAlive(), "Request caller did not exit");
      }
    }
  }

  @ParameterizedTest(name = "{0}: inline blocking continuation stalls I/O")
  @EnumSource(WaitStrategy.class)
  void waitingInsideCompletionCallbackOccupiesIoThread(WaitStrategy strategy) throws Exception {
    final var first = new CompletableFuture<String>();
    final var dependency = new CompletableFuture<String>();
    final var secondProcessedBy = new CompletableFuture<Thread>();
    final var callbackEntered = new CountDownLatch(1);
    final var supplier = strategy.supplier(dependency);
    // Registered before completion, so this non-async continuation runs on the I/O thread.
    final var continuation =
        first.thenApply(
            value -> {
              callbackEntered.countDown();
              return supplier.get();
            });
    try (final var io =
        new SocketEventLoop(
            message -> {
              if (message == 1) {
                first.complete("first response");
              } else {
                secondProcessedBy.complete(Thread.currentThread());
              }
            })) {
      try {
        io.send(1);
        assertTrue(callbackEntered.await(2, TimeUnit.SECONDS));
        awaitParked(io.thread);
        io.send(2);
        // The socket has more data, but the only reader is parked inside the callback.
        assertThrows(
            TimeoutException.class, () -> secondProcessedBy.get(200, TimeUnit.MILLISECONDS));
      } finally {
        // Rescue externally: a real dependency on the same stalled loop would deadlock.
        dependency.complete("released");
      }
      assertEquals("released", continuation.get(2, TimeUnit.SECONDS));
      assertSame(io.thread, secondProcessedBy.get(2, TimeUnit.SECONDS));
    }
  }

  private static void awaitParked(Thread thread) {
    final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
    while (thread.getState() != Thread.State.WAITING && System.nanoTime() < deadline) {
      LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
    }
    assertEquals(Thread.State.WAITING, thread.getState(), "Expected a parked future/latch waiter");
  }

  /**
   * A single platform thread selects and reads a real TCP socket, then completes futures inline.
   */
  private static final class SocketEventLoop implements AutoCloseable {
    private final SocketChannel sender;
    private final SocketChannel receiver;
    private final Selector selector;
    private final ExecutorService executor;
    private final Future<Void> loop;
    private final Thread thread;

    SocketEventLoop(Consumer<Byte> onMessage) throws Exception {
      selector = Selector.open();
      try (final var listener = ServerSocketChannel.open()) {
        listener.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
        sender = SocketChannel.open(listener.getLocalAddress());
        receiver = listener.accept();
      }
      receiver.configureBlocking(false);
      receiver.register(selector, SelectionKey.OP_READ);
      executor =
          Executors.newSingleThreadExecutor(
              Thread.ofPlatform().daemon().name("socket-io-loop").factory());
      thread = executor.submit(Thread::currentThread).get(2, TimeUnit.SECONDS);
      loop =
          executor.submit(
              () -> {
                final var buffer = ByteBuffer.allocate(1);
                while (true) {
                  selector.select();
                  final var keys = selector.selectedKeys().iterator();
                  while (keys.hasNext()) {
                    keys.next();
                    keys.remove();
                    final var read = receiver.read(buffer);
                    if (read < 0) {
                      return null;
                    }
                    if (read > 0) {
                      buffer.flip();
                      onMessage.accept(buffer.get());
                      buffer.clear();
                    }
                  }
                }
              });
      System.out.println("I/O selector: " + selector.getClass().getName());
    }

    void send(int message) throws IOException {
      final var buffer = ByteBuffer.wrap(new byte[] {(byte) message});
      while (buffer.hasRemaining()) {
        sender.write(buffer);
      }
    }

    @Override
    public void close() throws Exception {
      try {
        sender.close(); // EOF wakes the selector and exits the loop.
        loop.get(2, TimeUnit.SECONDS);
      } finally {
        receiver.close();
        selector.close();
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(2, TimeUnit.SECONDS), "I/O executor did not exit");
        thread.join(2000);
        assertFalse(thread.isAlive(), "I/O thread did not exit");
      }
    }
  }
}
