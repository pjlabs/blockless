package org.pjlabs.blockless.context.grpc;

import io.grpc.Context;
import org.pjlabs.blockless.ContextPropagator;

/**
 * Propagates gRPC {@link Context} across threads.
 */
public final class GrpcContextPropagator implements ContextPropagator {

    @Override
    public Object capture() {
        return Context.current();
    }

    @Override
    public Object attach(Object captured) {
        if (captured == null) {
            return null;
        }
        var ctx = (Context) captured;
        var previous = ctx.attach();
        return new GrpcRestore(ctx, previous);
    }

    @Override
    public void restore(Object previous) {
        if (previous == null) {
            return;
        }
        var state = (GrpcRestore) previous;
        state.captured().detach(state.previous());
    }

    private record GrpcRestore(Context captured, Context previous) {}
}
