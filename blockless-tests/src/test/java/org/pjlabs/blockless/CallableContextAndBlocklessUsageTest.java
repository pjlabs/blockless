package org.pjlabs.blockless;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.grpc.Context;
import io.opentelemetry.context.ContextKey;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.pjlabs.blockless.context.grpc.GrpcContextPropagator;
import org.pjlabs.blockless.context.opentelemetry.OpenTelemetryContextPropagator;
import org.pjlabs.blockless.context.slf4j.Slf4jMdcContextPropagator;
import org.slf4j.MDC;

/**
 * Documents typical usage: wrap a {@link Callable} with {@link CallableContext}, then run it on a
 * virtual thread and await the result with {@link Blockless#get(Callable)}.
 */
class CallableContextAndBlocklessUsageTest {

    @Test
    @DisplayName("Blockless.get(CallableContext.wrap(task, propagators)) — wrap then await on a virtual thread")
    void blocklessGetWithWrapCarriesMdcIntoCallable() {
        var mdc = new Slf4jMdcContextPropagator();
        MDC.clear();
        MDC.put("requestId", "req-42");

        // Capture current MDC and produce a Callable that will see it when Blockless runs it elsewhere.
        Callable<String> work = CallableContext.wrap(() -> "handled: " + MDC.get("requestId"), mdc);

        MDC.clear();
        assertNull(MDC.get("requestId"));

        String result = Blockless.get(work);
        assertEquals("handled: req-42", result);

        assertNull(MDC.get("requestId"));
    }

    @Test
    @DisplayName("CallableContext.capture(...).wrapCaptured(task) — snapshot now, run later with Blockless.get")
    void blocklessGetWithCaptureAndWrapCapturedDefersExecution() {
        var mdc = new Slf4jMdcContextPropagator();
        MDC.clear();
        MDC.put("tenant", "acme");

        // Snapshot propagator state once; the wrapped callable can be passed around and run later.
        CallableContext captured = CallableContext.capture(mdc);
        Callable<String> work = captured.wrapCaptured(() -> "tenant=" + MDC.get("tenant"));

        MDC.clear();

        assertEquals("tenant=acme", Blockless.get(work));
        assertNull(MDC.get("tenant"));
    }

    @Test
    @DisplayName("Multiple propagators: Blockless.get after CallableContext.wrap with MDC + gRPC + OTel")
    void blocklessGetWithSeveralPropagators() {
        MDC.clear();
        MDC.put("k", "mdc");

        Context.Key<String> grpcKey = Context.key("gk");
        Context grpcCtx = Context.current().withValue(grpcKey, "grpc");
        Context grpcPrev = grpcCtx.attach();

        ContextKey<String> otelKey = ContextKey.named("ok");
        var otelCtx = io.opentelemetry.context.Context.current().with(otelKey, "otel");

        Callable<String> work;
        try (var otelScope = otelCtx.makeCurrent()) {
            work = CallableContext.wrap(
                    () -> MDC.get("k")
                            + "|"
                            + grpcKey.get()
                            + "|"
                            + io.opentelemetry.context.Context.current().get(otelKey),
                    new Slf4jMdcContextPropagator(),
                    new GrpcContextPropagator(),
                    new OpenTelemetryContextPropagator());
            MDC.clear();
            grpcCtx.detach(grpcPrev);
        }

        assertEquals("mdc|grpc|otel", Blockless.get(work));
        assertNull(MDC.get("k"));
    }
}
