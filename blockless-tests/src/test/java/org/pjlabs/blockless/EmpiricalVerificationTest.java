package org.pjlabs.blockless;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.pjlabs.blockless.context.slf4j.Slf4jMdcContextPropagator;
import org.slf4j.MDC;

/**
 * Empirical tests that prove blockless claims by demonstrating both the problem and the solution.
 * Organized as "without blockless" (the failure) vs "with blockless" (the fix).
 */
class EmpiricalVerificationTest {

    @BeforeEach
    void setUp() {
        MDC.clear();
    }

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Nested
    class WithoutBlockless {

        @Test
        void virtualThreadLosesMdc() throws Exception {
            MDC.put("traceId", "abc-123");
            var captured = new AtomicReference<String>();

            var thread = Thread.startVirtualThread(() -> captured.set(MDC.get("traceId")));
            thread.join();

            assertNull(captured.get(), "plain virtual thread must lose MDC — this is the problem blockless solves");
        }

        @Test
        void platformThreadLosesMdc() throws Exception {
            MDC.put("traceId", "abc-123");
            var captured = new AtomicReference<String>();

            try (var executor = Executors.newFixedThreadPool(1)) {
                var future = executor.submit(() -> captured.set(MDC.get("traceId")));
                future.get();
            }

            assertNull(captured.get(), "plain platform thread pool must lose MDC — not a virtual thread specific problem");
        }
    }

    @Nested
    class WithBlockless {

        @Test
        void callableContextPreservesMdc() throws Exception {
            MDC.put("traceId", "abc-123");

            var wrapped = CallableContext.wrap(
                    () -> MDC.get("traceId"),
                    new Slf4jMdcContextPropagator());

            MDC.clear();

            var result = Blockless.get(wrapped);
            assertEquals("abc-123", result, "CallableContext.wrap must propagate MDC to the executing thread");
        }

        @Test
        void runnableContextPreservesMdc() throws Exception {
            MDC.put("traceId", "abc-123");
            var captured = new AtomicReference<String>();

            var wrapped = RunnableContext.wrap(
                    () -> captured.set(MDC.get("traceId")),
                    new Slf4jMdcContextPropagator());

            MDC.clear();

            var thread = Thread.startVirtualThread(wrapped);
            thread.join();

            assertEquals("abc-123", captured.get(), "RunnableContext.wrap must propagate MDC to the executing thread");
        }

        @Test
        void blocklessGetRunsOnVirtualThread() {
            var isVirtual = Blockless.get(() -> Thread.currentThread().isVirtual());
            assertTrue(isVirtual, "Blockless.get(Callable) must execute on a virtual thread");
        }

        @Test
        void blocklessGetDoesNotPinPlatformThreads() {
            // Run 100 concurrent blocking tasks each sleeping 50ms.
            // If platform threads were pinned, this would take ~5000ms (100 * 50ms serial).
            // With virtual threads, all 100 run concurrently and complete in ~50ms + overhead.
            int taskCount = 100;
            long sleepMs = 50;
            var latch = new CountDownLatch(taskCount);
            var threads = new Thread[taskCount];

            long start = System.nanoTime();
            for (int i = 0; i < taskCount; i++) {
                threads[i] = Thread.startVirtualThread(() -> {
                    Blockless.get(() -> {
                        Thread.sleep(sleepMs);
                        return null;
                    });
                    latch.countDown();
                });
            }

            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            long elapsedMs = (System.nanoTime() - start) / 1_000_000;

            // With true concurrency, elapsed should be well under taskCount * sleepMs.
            // Allow generous headroom (10x the sleep time) but nowhere near serial execution.
            assertTrue(elapsedMs < sleepMs * 10,
                    "expected concurrent execution (~" + sleepMs + "ms) but took " + elapsedMs
                            + "ms — suggests platform thread pinning");
        }

        @Test
        void callingThreadContextUnchangedAfterGet() throws Exception {
            MDC.put("traceId", "parent-value");

            Blockless.get(CallableContext.wrap(() -> {
                assertEquals("parent-value", MDC.get("traceId"));
                return null;
            }, new Slf4jMdcContextPropagator()));

            assertEquals("parent-value", MDC.get("traceId"),
                    "calling thread's MDC must be unchanged after Blockless.get completes");
        }
    }
}
