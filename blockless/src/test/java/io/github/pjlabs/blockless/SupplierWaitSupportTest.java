package io.github.pjlabs.blockless;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(10)
class SupplierWaitSupportTest {
  @Test
  void nestedHandlersAreRestoredAfterFailureAndDoNotLeakOutsideTheScope() {
    final var release = new CountDownLatch(1);
    final var supplier =
        Blockless.supplier(
            () -> {
              release.await();
              return "ready";
            });
    final var outer = new IllegalStateException("outer");
    final var inner = new IllegalStateException("inner");
    try {
      SupplierWaitSupport.withHandler(
          pending -> {
            throw outer;
          },
          () -> {
            assertSame(
                inner,
                assertThrows(
                    IllegalStateException.class,
                    () ->
                        SupplierWaitSupport.withHandler(
                            pending -> {
                              throw inner;
                            },
                            supplier::get)));
            assertSame(outer, assertThrows(IllegalStateException.class, supplier::get));
            return null;
          });
    } finally {
      release.countDown();
    }
    assertEquals("ready", supplier.get());
  }

  @Test
  void rejectsAHandlerThatReturnsWithoutMakingTheResultReadable() {
    final var release = new CountDownLatch(1);
    final var supplier =
        Blockless.supplier(
            () -> {
              release.await();
              return "ready";
            });
    try {
      assertThrows(
          IllegalStateException.class,
          () -> SupplierWaitSupport.withHandler(pending -> {}, supplier::get));
    } finally {
      release.countDown();
    }
    assertEquals("ready", supplier.get());
  }

  @Test
  void completedSupplierBypassesTheHandler() {
    final var supplier = Blockless.supplier(() -> "ready");
    assertEquals("ready", supplier.get());
    assertEquals(
        "ready",
        SupplierWaitSupport.withHandler(
            pending -> fail("completed reads must not invoke the handler"), supplier::get));
  }
}
