package io.github.pjlabs.blockless;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.github.pjlabs.blockless.context.grpc.GrpcContextPropagator;
import io.github.pjlabs.blockless.context.opentelemetry.OpenTelemetryContextPropagator;
import io.github.pjlabs.blockless.context.slf4j.Slf4jMdcContextPropagator;
import io.grpc.Context;
import io.opentelemetry.context.ContextKey;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

class CallableContextIntegrationTest {

  @Test
  void propagatesMdcAcrossThreadsAndRestores() {
    final var mdc = new Slf4jMdcContextPropagator();
    MDC.clear();
    MDC.put("k", "v");

    final var wrapped = CallableContext.wrap(() -> MDC.get("k"), mdc);

    MDC.clear();
    assertNull(MDC.get("k"));

    final var result = Blockless.get(wrapped);
    assertEquals("v", result);

    assertNull(MDC.get("k"));
  }

  @Test
  void propagatesGrpcContextAcrossThreadsAndRestores() {
    final var grpc = new GrpcContextPropagator();
    Context.Key<String> key = Context.key("k");

    final var ctx = Context.current().withValue(key, "v");
    final var prev = ctx.attach();
    try {
      final var wrapped = CallableContext.wrap((Callable<String>) key::get, grpc);
      final var result = Blockless.get(wrapped);
      assertEquals("v", result);
      assertEquals("v", key.get(), "submitting thread gRPC context unchanged after get");
    } finally {
      ctx.detach(prev);
    }
  }

  @Test
  void restoresWorkerThreadPreExistingMdc() throws Exception {
    final var mdc = new Slf4jMdcContextPropagator();
    MDC.clear();
    MDC.put("k", "parent-value");

    final var wrapped = CallableContext.wrap(() -> MDC.get("k"), mdc);

    MDC.clear();

    // Simulate a worker thread that already has its own MDC context
    final var workerMdcAfter = new AtomicReference<String>();
    final var workerThread =
        Thread.startVirtualThread(
            () -> {
              MDC.put("k", "worker-value");
              try {
                wrapped.call();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
              // After wrapped callable completes, worker's own MDC should be restored
              workerMdcAfter.set(MDC.get("k"));
            });
    workerThread.join();

    assertEquals(
        "worker-value",
        workerMdcAfter.get(),
        "worker thread's pre-existing MDC must be restored after wrapped callable executes");
  }

  @Test
  void propagatesOpenTelemetryContextAcrossThreadsAndRestores() {
    final var otel = new OpenTelemetryContextPropagator();
    ContextKey<String> key = ContextKey.named("k");

    final var ctx = io.opentelemetry.context.Context.current().with(key, "v");
    try (var scope = ctx.makeCurrent()) {
      final var wrapped =
          CallableContext.wrap(() -> io.opentelemetry.context.Context.current().get(key), otel);
      final var result = Blockless.get(wrapped);
      assertEquals("v", result);
      assertEquals(
          "v",
          io.opentelemetry.context.Context.current().get(key),
          "submitting thread OTel context unchanged after get");
    }
  }
}
