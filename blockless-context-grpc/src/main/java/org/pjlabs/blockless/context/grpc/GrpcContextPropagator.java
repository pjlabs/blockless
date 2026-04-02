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
        if (captured == null || !(captured instanceof Context context)) {
            return null;
        }
        return new GrpcContext(context, context.attach());
    }

    @Override
    public void restore(Object previous) {
        if (previous == null || !(previous instanceof GrpcContext grpcContext)) {
            return;
        }
        grpcContext.restore();
    }

    private record GrpcContext(Context context, Context previous) {
        void restore() {
            context.detach(previous);
        }
    }
}
