package io.github.pjlabs.blockless;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * An experimental fixed platform pool whose workers can launch queued tasks on virtual threads
 * during supported Blockless waits. The waiting caller keeps its thread and stack.
 *
 * <p>The worker count limits platform workers; {@code maxHelped} separately limits helped tasks.
 * Use {@link PropagatingExecutorService} for explicit context propagation. Helped threads inherit
 * inheritable thread locals and the context class loader at submission time.
 *
 * <p>Shutdown starts termination without waiting. Termination includes actual task exit and queued
 * Future cancellation hooks. Independently started supplier bodies retain their separate lifetime.
 * If a helper cannot start, that wait throws and the queued task remains available to platform
 * workers. This API is experimental and may change.
 */
public final class LazyHelpingExecutor extends AbstractExecutorService {
  private final ReentrantLock state = new ReentrantLock();
  private final Condition progress = state.newCondition();
  private final LinkedBlockingDeque<Runnable> queue = new LinkedBlockingDeque<>();
  // Authoritative ownership from acceptance until task exit or cancellation cleanup.
  private final Set<Dispatch> pending = new LinkedHashSet<>();
  private final ThreadPoolExecutor platform;
  private final int maxHelped;
  private final Consumer<Thread> helperStarter;
  private final AtomicLong sequence = new AtomicLong();
  private int runningHelped;
  private boolean shutdown;
  private boolean stopped;

  /** Creates fixed non-daemon platform workers with unbounded helping. */
  public static ExecutorService newFixedThreadPool(int workers) {
    return newFixedThreadPool(workers, Executors.defaultThreadFactory());
  }

  /** Creates fixed platform workers from {@code factory} with unbounded helping. */
  public static ExecutorService newFixedThreadPool(int workers, ThreadFactory factory) {
    return new LazyHelpingExecutor(workers, factory, Integer.MAX_VALUE, Thread::start);
  }

  /** Creates fixed non-daemon platform workers and at most {@code maxHelped} helped tasks. */
  public static ExecutorService newFixedThreadPool(int workers, int maxHelped) {
    return newFixedThreadPool(workers, maxHelped, Executors.defaultThreadFactory());
  }

  /** Creates fixed platform workers from {@code factory}, with a separate cap on helped tasks. */
  public static ExecutorService newFixedThreadPool(
      int workers, int maxHelped, ThreadFactory factory) {
    return new LazyHelpingExecutor(workers, factory, maxHelped, Thread::start);
  }

  // Test seam: the starter either starts the supplied thread or throws before starting it.
  LazyHelpingExecutor(int workers, ThreadFactory factory, int maxHelped, Consumer<Thread> starter) {
    if (maxHelped < 1) throw new IllegalArgumentException("maxHelped must be positive");
    Objects.requireNonNull(factory, "factory");
    this.maxHelped = maxHelped;
    helperStarter = Objects.requireNonNull(starter, "starter");
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
    HelpingWait.enabled = true;
  }

  @Override
  public void execute(Runnable task) {
    final var dispatch = new Dispatch(Objects.requireNonNull(task, "task"));
    state.lock();
    try {
      if (shutdown) throw new RejectedExecutionException("Executor shut down");
      pending.add(dispatch);
      try {
        platform.execute(dispatch);
      } catch (RuntimeException | Error failure) {
        dispatch.status = Status.CANCELLING;
        pending.remove(dispatch);
        queue.remove(dispatch);
        throw failure;
      }
      progress.signalAll();
    } finally {
      state.unlock();
    }
  }

  void signalProgress() {
    state.lock();
    try {
      progress.signalAll();
    } finally {
      state.unlock();
    }
  }

  void helpUntil(CountDownLatch latch) throws InterruptedException {
    state.lockInterruptibly();
    try {
      while (latch.getCount() != 0) {
        if (Thread.interrupted()) throw new InterruptedException();
        if (stopped) throw new InterruptedException("Executor stopped");
        if (queue.isEmpty() || runningHelped == maxHelped) {
          progress.await();
        } else {
          final var dispatch = (Dispatch) queue.poll();
          // A platform worker may have taken the queued task first.
          if (dispatch == null) continue;
          startHelped(dispatch);
        }
      }
    } finally {
      state.unlock();
    }
  }

  // Called with state held: claim and start form one transition for shutdownNow.
  private void startHelped(Dispatch dispatch) {
    dispatch.status = Status.RUNNING;
    dispatch.runner = dispatch.virtual;
    runningHelped++;
    try {
      helperStarter.accept(dispatch.virtual);
    } catch (RuntimeException | Error failure) {
      runningHelped--;
      dispatch.status = Status.QUEUED;
      dispatch.runner = null;
      queue.addFirst(dispatch);
      // Unwind this wait so the platform worker can run the requeued task.
      throw failure;
    }
  }

  @Override
  public void shutdown() {
    final boolean idle;
    state.lock();
    try {
      shutdown = true;
      idle = pending.isEmpty();
      progress.signalAll();
    } finally {
      state.unlock();
    }
    // Keep platform workers alive until every accepted task and cancellation hook has exited.
    if (idle) platform.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow() {
    final var cancelled = new ArrayList<Dispatch>();
    final var interrupted = new ArrayList<Thread>();
    state.lock();
    try {
      shutdown = true;
      stopped = true;
      for (final var dispatch : pending) {
        if (dispatch.status == Status.QUEUED) {
          dispatch.status = Status.CANCELLING;
          cancelled.add(dispatch);
        } else if (dispatch.status == Status.RUNNING) {
          interrupted.add(dispatch.runner);
        }
      }
      queue.clear();
      progress.signalAll();
    } finally {
      state.unlock();
    }
    Throwable failure = null;
    for (final var thread : interrupted) {
      try {
        thread.interrupt();
      } catch (RuntimeException | Error thrown) {
        if (failure == null) failure = thrown;
        else if (failure != thrown) failure.addSuppressed(thrown);
      }
    }
    // Retain each cancellation in pending until its user-defined hook has returned.
    for (final var dispatch : cancelled) {
      try {
        if (dispatch.task instanceof Future<?> future) future.cancel(false);
      } catch (RuntimeException | Error thrown) {
        if (failure == null) failure = thrown;
        else if (failure != thrown) failure.addSuppressed(thrown);
      } finally {
        dispatch.finished(false);
      }
    }
    shutdown();
    if (failure instanceof RuntimeException runtime) throw runtime;
    if (failure instanceof Error error) throw error;
    return cancelled.stream().map(dispatch -> dispatch.task).toList();
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
    return platform.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return platform.awaitTermination(timeout, unit);
  }

  private enum Status {
    QUEUED,
    RUNNING,
    CANCELLING
  }

  private final class Dispatch implements Runnable {
    private final Runnable task;
    private final Thread virtual;
    private Thread runner;
    private Status status = Status.QUEUED;

    private Dispatch(Runnable task) {
      this.task = task;
      virtual =
          Thread.ofVirtual()
              .name("lazy-help-" + sequence.incrementAndGet())
              .unstarted(this::runHelped);
    }

    @Override
    public void run() {
      state.lock();
      try {
        if (status != Status.QUEUED) return;
        status = Status.RUNNING;
        runner = Thread.currentThread();
      } finally {
        state.unlock();
      }
      HelpingWait.CURRENT.set(LazyHelpingExecutor.this);
      try {
        task.run();
      } finally {
        HelpingWait.CURRENT.remove();
        finished(false);
      }
    }

    private void runHelped() {
      try {
        task.run();
      } finally {
        finished(true);
      }
    }

    private void finished(boolean helped) {
      final boolean idle;
      state.lock();
      try {
        if (helped) runningHelped--;
        pending.remove(this);
        idle = shutdown && pending.isEmpty();
        progress.signalAll();
      } finally {
        state.unlock();
      }
      if (idle) platform.shutdown();
    }
  }
}
