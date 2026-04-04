package org.pjlabs.blockless;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.pjlabs.blockless.context.slf4j.Slf4jMdcContextPropagator;
import org.slf4j.MDC;

class RunnableContextIntegrationTest {

  @Test
  void propagatesMdcAndRestoresCallingThread() throws Exception {
    final var mdc = new Slf4jMdcContextPropagator();
    MDC.clear();
    MDC.put("k", "v");

    final var captured = new AtomicReference<String>();
    final var wrapped = RunnableContext.wrap(() -> captured.set(MDC.get("k")), mdc);

    // Clear MDC after wrapping — captured state should still propagate
    MDC.clear();
    assertNull(MDC.get("k"));

    // Run on a virtual thread
    final var thread = Thread.startVirtualThread(wrapped);
    thread.join();

    assertEquals("v", captured.get(), "wrapped runnable should see captured MDC");
    assertNull(MDC.get("k"), "calling thread MDC should remain cleared");
  }

  @Test
  void restoresWorkerThreadPreExistingContext() throws Exception {
    final var mdc = new Slf4jMdcContextPropagator();
    MDC.clear();
    MDC.put("k", "parent-value");

    final var wrapped =
        RunnableContext.wrap(
            () -> {
              // Inside the wrapped runnable, should see "parent-value"
              assertEquals("parent-value", MDC.get("k"));
            },
            mdc);

    MDC.clear();

    // Simulate a worker thread that already has its own MDC context
    final var workerMdcAfter = new AtomicReference<String>();
    final var workerThread =
        Thread.startVirtualThread(
            () -> {
              MDC.put("k", "worker-value");
              wrapped.run();
              // After wrapped runnable completes, worker's own MDC should be restored
              workerMdcAfter.set(MDC.get("k"));
            });
    workerThread.join();

    assertEquals(
        "worker-value",
        workerMdcAfter.get(),
        "worker thread's pre-existing MDC must be restored after wrapped runnable executes");
  }
}
