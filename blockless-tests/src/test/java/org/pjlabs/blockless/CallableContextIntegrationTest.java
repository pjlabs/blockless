package org.pjlabs.blockless;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.grpc.Context;
import io.opentelemetry.context.ContextKey;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.Test;
import org.pjlabs.blockless.context.grpc.GrpcContextPropagator;
import org.pjlabs.blockless.context.opentelemetry.OpenTelemetryContextPropagator;
import org.pjlabs.blockless.context.slf4j.Slf4jMdcContextPropagator;
import org.slf4j.MDC;

class CallableContextIntegrationTest {

    @Test
    void propagatesMdcAcrossThreadsAndRestores() {
        var mdc = new Slf4jMdcContextPropagator();
        MDC.clear();
        MDC.put("k", "v");

        var wrapped = CallableContext.wrap(() -> MDC.get("k"), mdc);

        MDC.clear();
        assertNull(MDC.get("k"));

        var result = Blockless.get(wrapped);
        assertEquals("v", result);

        assertNull(MDC.get("k"));
    }

    @Test
    void propagatesGrpcContextAcrossThreadsAndRestores() {
        var grpc = new GrpcContextPropagator();
        Context.Key<String> key = Context.key("k");

        var ctx = Context.current().withValue(key, "v");
        var prev = ctx.attach();
        try {
            var wrapped = CallableContext.wrap((Callable<String>) key::get, grpc);
            var result = Blockless.get(wrapped);
            assertEquals("v", result);
            assertEquals("v", key.get(), "submitting thread gRPC context unchanged after get");
        } finally {
            ctx.detach(prev);
        }
    }

    @Test
    void propagatesOpenTelemetryContextAcrossThreadsAndRestores() {
        var otel = new OpenTelemetryContextPropagator();
        ContextKey<String> key = ContextKey.named("k");

        var ctx = io.opentelemetry.context.Context.current().with(key, "v");
        try (var scope = ctx.makeCurrent()) {
            var wrapped = CallableContext.wrap(
                    () -> io.opentelemetry.context.Context.current().get(key), otel);
            var result = Blockless.get(wrapped);
            assertEquals("v", result);
            assertEquals(
                    "v",
                    io.opentelemetry.context.Context.current().get(key),
                    "submitting thread OTel context unchanged after get");
        }
    }
}
