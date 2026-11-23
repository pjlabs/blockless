package io.github.pjlabs.blockless;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

/** Lets only this executor's platform workers help their queue while waiting for an outcome. */
final class HelpingWait {
  static final ThreadLocal<LazyHelpingExecutor> CURRENT = new ThreadLocal<>();
  private static final ConcurrentHashMap<CountDownLatch, CopyOnWriteArrayList<Registration>>
      WAITERS = new ConcurrentHashMap<>();

  private HelpingWait() {}

  static void await(CountDownLatch latch) throws InterruptedException {
    // Avoid creating a ThreadLocal entry for ordinary virtual-thread callers.
    if (Thread.currentThread().isVirtual()) {
      latch.await();
      return;
    }
    final var owner = CURRENT.get();
    if (owner == null) {
      latch.await();
      return;
    }
    final var registration = new Registration(owner);
    WAITERS.compute(
        latch,
        (ignored, registrations) -> {
          final var result =
              registrations == null ? new CopyOnWriteArrayList<Registration>() : registrations;
          result.add(registration);
          return result;
        });
    try {
      owner.helpUntil(latch);
    } finally {
      WAITERS.computeIfPresent(
          latch,
          (ignored, registrations) -> {
            registrations.remove(registration);
            return registrations.isEmpty() ? null : registrations;
          });
    }
  }

  static void signal(CountDownLatch latch) {
    final var registrations = WAITERS.get(latch);
    if (registrations != null) {
      for (final var registration : registrations) registration.owner.signalProgress();
    }
  }

  private record Registration(LazyHelpingExecutor owner) {}
}
