package io.github.pjlabs.blockless;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.UnaryOperator;

/**
 * An experimental executor whose waiting platform workers dispatch queued tasks onto virtual
 * threads.
 *
 * <p>Tasks normally start on the platform pool. While one calls a pending {@link Blockless}
 * supplier or {@link Parallel#async} supplier, it helps its own queue until the result is
 * available. Its stack, thread identity and thread-local state stay on the original worker; helped
 * tasks run on separate virtual threads. Other blocking calls do not help.
 *
 * <p>If starting a helped task throws a runtime exception, the platform worker ends helping for
 * that wait and continues waiting for its own result.
 *
 * <p>The worker count limits platform workers. Even one platform worker can dispatch concurrent
 * virtual tasks. The optional {@code maxHelped} parameter caps concurrently running helped tasks
 * across the executor. Size it to the downstream resource, such as the database connection pool.
 * Overloads that omit {@code maxHelped} leave helping unbounded.
 *
 * <p>On Java 21, helped tasks that block inside {@code synchronized} code pin their carrier. Use
 * Java 24 or later for such workloads (see <a href="https://openjdk.org/jeps/491">JEP 491</a>).
 *
 * <p>No context propagators are required. Use {@link PropagatingExecutorService} when submitted
 * tasks need explicit context capture.
 *
 * <p>Shutdown waits for actual submitted-task cleanup, including helped tasks whose Futures were
 * cancelled. Independently started supplier bodies retain their existing separate lifetime. Uses
 * public Java 21 APIs only. This API is experimental and may change.
 */
public final class LazyHelpingExecutor extends AbstractExecutorService {
  private final ReentrantLock state = new ReentrantLock();
  private final Condition progress = state.newCondition();
  private final LinkedBlockingDeque<Runnable> queue = new LinkedBlockingDeque<>();
  private final ThreadPoolExecutor platform;
  private final ExecutorService helped;
  private final Semaphore helpedPermits;
  private final ThreadLocal<Dispatch> launching = new ThreadLocal<>();
  private final List<Thread> nativeRunners = new ArrayList<>();
  private final AtomicLong sequence = new AtomicLong();
  private final Runnable finisher = this::finishShutdown;
  private int waitingHelpers;
  private boolean shutdown;
  private boolean stopped;
  private boolean finisherSubmitted;

  /** Creates a fixed platform pool with a custom thread factory and unbounded helping. */
  public static ExecutorService newFixedThreadPool(int workers, ThreadFactory factory) {
    return new LazyHelpingExecutor(workers, factory, null, UnaryOperator.identity());
  }

  /** Creates a fixed pool of non-daemon platform workers with unbounded helping. */
  public static ExecutorService newFixedThreadPool(int workers) {
    return newFixedThreadPool(workers, Executors.defaultThreadFactory());
  }

  /**
   * Creates an executor with fixed non-daemon platform workers and a cap on helping.
   *
   * @param workers the number of platform workers
   * @param maxHelped the maximum number of concurrently running helped tasks
   * @return the executor
   * @throws IllegalArgumentException if {@code workers} or {@code maxHelped} is less than 1
   */
  public static ExecutorService newFixedThreadPool(int workers, int maxHelped) {
    return newFixedThreadPool(workers, maxHelped, Executors.defaultThreadFactory());
  }

  /**
   * Creates an executor with fixed platform workers from the supplied factory and a cap on helping.
   *
   * @param workers the number of platform workers
   * @param maxHelped the maximum number of concurrently running helped tasks
   * @param factory the factory for platform workers
   * @return the executor
   * @throws IllegalArgumentException if {@code workers} or {@code maxHelped} is less than 1
   */
  public static ExecutorService newFixedThreadPool(
      int workers, int maxHelped, ThreadFactory factory) {
    if (maxHelped < 1) throw new IllegalArgumentException("maxHelped must be positive");
    return new LazyHelpingExecutor(
        workers, factory, new Semaphore(maxHelped), UnaryOperator.identity());
  }

  // The wrapper lets tests inject failures in the helped thread factory.
  LazyHelpingExecutor(
      int workers,
      ThreadFactory factory,
      Semaphore helpedPermits,
      UnaryOperator<ThreadFactory> wrapHelpedFactory) {
    Objects.requireNonNull(factory, "factory");
    this.helpedPermits = helpedPermits;
    platform =
        new ThreadPoolExecutor(
            workers,
            workers,
            0,
            TimeUnit.SECONDS,
            queue,
            task -> {
              final var thread = Objects.requireNonNull(factory.newThread(task));
              if (thread.isVirtual())
                throw new IllegalArgumentException("Platform factory required");
              return thread;
            });
    // Reading launching relies on newThreadPerTaskExecutor calling newThread synchronously
    // on the thread that calls execute.
    final ThreadFactory helpedFactory =
        taskRunner -> {
          final var dispatch = Objects.requireNonNull(launching.get(), "missing helped dispatch");
          dispatch.deferred.target = taskRunner;
          return dispatch.virtual;
        };
    helped = Executors.newThreadPerTaskExecutor(wrapHelpedFactory.apply(helpedFactory));
    HelpingWait.enabled = true;
  }

  @Override
  public void execute(Runnable task) {
    Objects.requireNonNull(task, "task");
    final var dispatch = new Dispatch(task);
    state.lock();
    try {
      if (shutdown) throw new RejectedExecutionException("Executor shut down");
      platform.execute(dispatch);
      if (waitingHelpers > 0) progress.signalAll();
    } finally {
      state.unlock();
    }
  }

  void signalProgress() {
    state.lock();
    try {
      if (waitingHelpers > 0) progress.signalAll();
    } finally {
      state.unlock();
    }
  }

  // Returns false when the caller should finish waiting directly on the latch.
  boolean helpUntil(CountDownLatch latch) throws InterruptedException {
    while (true) {
      Dispatch abandoned = null;
      Throwable launchFailure = null;
      state.lockInterruptibly();
      try {
        if (latch.getCount() == 0) return true;
        if (!(queue.peek() instanceof Dispatch)
            || (helpedPermits != null && !helpedPermits.tryAcquire())) {
          waitingHelpers++;
          try {
            progress.await();
          } finally {
            waitingHelpers--;
          }
          continue;
        }
        // A normal platform worker may have taken the peeked task in the meantime.
        final var candidate = queue.poll();
        if (!(candidate instanceof Dispatch dispatch)) {
          if (helpedPermits != null) helpedPermits.release();
          if (candidate != null) queue.add(candidate);
          continue;
        }
        // Keep acceptance/lifecycle state locked through registration with the helper owner.
        try {
          launching.set(dispatch);
          helped.execute(dispatch::runHelped);
        } catch (Throwable failure) {
          if (helpedPermits != null) helpedPermits.release();
          abandoned = dispatch;
          launchFailure = failure;
          if (requeueFailedLaunch(dispatch, failure)) abandoned = null;
          if (waitingHelpers > 0) progress.signalAll();
          if (failure instanceof Error error) throw error;
          return false;
        } finally {
          launching.remove();
        }
      } finally {
        state.unlock();
        if (abandoned != null) {
          // Foreign FutureTask.done hooks can call back into this executor.
          try {
            abandoned.cancelBeforeStart();
          } catch (Throwable failure) {
            appendFailure(launchFailure, failure);
          }
        }
      }
    }
  }

  private boolean requeueFailedLaunch(Dispatch dispatch, Throwable launchFailure) {
    // A failed start can leave the virtual thread unable to run on retry.
    if (stopped || dispatch.virtual.getState() != Thread.State.NEW) return false;
    try {
      // Keep the dispatch ahead of the shutdown finisher.
      return queue.offerFirst(dispatch);
    } catch (Throwable failure) {
      appendFailure(launchFailure, failure);
      return false;
    }
  }

  @Override
  public void shutdown() {
    state.lock();
    try {
      if (shutdown) return;
      shutdown = true;
      submitFinisher();
      platform.shutdown();
      progress.signalAll();
    } finally {
      state.unlock();
    }
  }

  @Override
  public List<Runnable> shutdownNow() {
    final var abandoned = new ArrayList<Runnable>();
    final var cancelled = new ArrayList<Dispatch>();
    final List<Thread> interrupted;
    state.lock();
    try {
      if (stopped) return List.of();
      shutdown = true;
      stopped = true;
      for (final var task : queue.toArray(Runnable[]::new)) {
        if (task instanceof Dispatch dispatch && queue.remove(task)) {
          abandoned.add(dispatch.task);
          cancelled.add(dispatch);
        }
      }
      interrupted = List.copyOf(nativeRunners);
      submitFinisher();
      progress.signalAll();
    } finally {
      state.unlock();
    }
    Throwable failure = null;
    try {
      helped.shutdownNow();
    } catch (Throwable thrown) {
      failure = appendFailure(failure, thrown);
    }
    for (final var thread : interrupted) {
      try {
        thread.interrupt();
      } catch (Throwable thrown) {
        failure = appendFailure(failure, thrown);
      }
    }
    try {
      // Retain the original pool's last worker to join helped tasks; create no extra keeper.
      platform.shutdown();
    } catch (Throwable thrown) {
      failure = appendFailure(failure, thrown);
    }
    // Foreign FutureTask.done hooks are user code and must run outside our state lock.
    // One failing hook must not leave the remaining accepted Futures permanently pending.
    for (final var dispatch : cancelled) {
      try {
        dispatch.cancelBeforeStart();
      } catch (Throwable thrown) {
        failure = appendFailure(failure, thrown);
      }
    }
    if (failure instanceof RuntimeException runtime) throw runtime;
    if (failure instanceof Error error) throw error;
    if (failure != null) throw new RuntimeException(failure);
    return abandoned;
  }

  private static Throwable appendFailure(Throwable first, Throwable next) {
    if (first == null) return next;
    if (first != next) first.addSuppressed(next);
    return first;
  }

  private void submitFinisher() {
    if (finisherSubmitted) return;
    finisherSubmitted = true;
    // platform.shutdownNow is intentionally unused, so accepted cleanup is not discarded.
    platform.execute(finisher);
  }

  private void finishShutdown() {
    var interrupted = false;
    state.lock();
    try {
      while (!nativeRunners.isEmpty()) {
        try {
          progress.await();
        } catch (InterruptedException ignored) {
          interrupted = true;
        }
      }
      helped.shutdown();
    } finally {
      state.unlock();
    }
    while (true) {
      try {
        if (helped.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) break;
      } catch (InterruptedException ignored) {
        interrupted = true;
      }
    }
    if (interrupted) Thread.currentThread().interrupt();
  }

  @Override
  public boolean isShutdown() {
    state.lock();
    try {
      return shutdown;
    } finally {
      state.unlock();
    }
  }

  @Override
  public boolean isTerminated() {
    return platform.isTerminated() && helped.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    final var nanos = unit.toNanos(timeout);
    final var start = System.nanoTime();
    if (!platform.awaitTermination(nanos, TimeUnit.NANOSECONDS)) return false;
    return helped.awaitTermination(
        Math.max(0, nanos - (System.nanoTime() - start)), TimeUnit.NANOSECONDS);
  }

  // Create the unstarted virtual thread at submit time so it inherits the submitter's
  // InheritableThreadLocals and context class loader.
  private final class Dispatch implements Runnable {
    private final Runnable task;
    private final Deferred deferred = new Deferred();
    private final Thread virtual;

    private Dispatch(Runnable task) {
      this.task = task;
      virtual =
          Thread.ofVirtual().name("lazy-help-" + sequence.incrementAndGet()).unstarted(deferred);
    }

    @Override
    public void run() {
      final var thread = Thread.currentThread();
      final boolean skip;
      state.lock();
      try {
        skip = stopped;
        if (!skip) nativeRunners.add(thread);
      } finally {
        state.unlock();
      }
      if (skip) {
        cancelBeforeStart();
        return;
      }
      HelpingWait.CURRENT.set(LazyHelpingExecutor.this);
      try {
        task.run();
      } finally {
        HelpingWait.CURRENT.remove();
        state.lock();
        try {
          nativeRunners.remove(thread);
          progress.signalAll();
        } finally {
          state.unlock();
        }
      }
    }

    private void runHelped() {
      try {
        task.run();
      } finally {
        if (helpedPermits != null) {
          helpedPermits.release();
          signalProgress();
        }
      }
    }

    private void cancelBeforeStart() {
      if (task instanceof Future<?> future) future.cancel(false);
    }
  }

  private static final class Deferred implements Runnable {
    private Runnable target;

    @Override
    public void run() {
      Objects.requireNonNull(target, "unbound helper").run();
    }
  }
}
