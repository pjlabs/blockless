package org.pjlabs.blockless;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class Blockless {

    /**
     * Returns a supplier that will block until the completion stage is complete.
     * Use when you want to run the completion stage now and get the result later in execution.
     * @param stage the completion stage
     * @param <T> the type of the value
     * @return a supplier that will block until the completion stage is complete
     */
    public static <T> Supplier<T> supplier(CompletionStage<T> stage) {
        var result = new AtomicReference<T>();
        var throwable = new AtomicReference<Throwable>();
        var latch = new CountDownLatch(1);
        stage.whenComplete((T v, Throwable t) -> {
            if (t != null) {
                throwable.set(t);
            } else {
                result.set(v);
            }
            latch.countDown();
        });
        var thread = Thread.startVirtualThread(() -> {
            try {
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        });
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        
        if (throwable.get() != null) {
            throw new RuntimeException(throwable.get());
        }
        return () -> result.get();
    }

    /**
     * Returns the value of the completion stage.
     * Use when you want to run the completion stage now and get the result before continuing execution.
     * @param stage the completion stage
     * @param <T> the type of the value
     * @return the value of the completion stage
     */
    public static <T> T get(CompletionStage<T> stage) {
        return supplier(stage).get();
    }

}