package org.pjlabs.blockless;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

class BlocklessTest {

    @Test
    void getReturnsCompletedStageValue() {
        var stage = CompletableFuture.completedFuture("ok");
        assertEquals("ok", Blockless.get(stage));
    }

    @Test
    void getWaitsForDelayedStage() {
        var stage = CompletableFuture.supplyAsync(
                () -> "later",
                CompletableFuture.delayedExecutor(50, TimeUnit.MILLISECONDS));
        assertEquals("later", Blockless.get(stage));
    }

    @Test
    void callReturnsCallableValue() {
        assertEquals("ok", Blockless.get(() -> "ok"));
    }

    @Test
    void callWaitsForDelayedCallable() {
        Callable<String> callable = () -> {
            Thread.sleep(50);
            return "later";
        };
        assertEquals("later", Blockless.get(callable));
    }
}
