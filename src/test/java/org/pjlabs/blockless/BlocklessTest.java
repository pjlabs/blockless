package org.pjlabs.blockless;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

class BlocklessTest {

    @Test
    void getReturnsCompletedStageValue() {
        var stage = CompletableFuture.completedFuture("ok");
        assertEquals("ok", Blockless.get(stage));
    }
}
