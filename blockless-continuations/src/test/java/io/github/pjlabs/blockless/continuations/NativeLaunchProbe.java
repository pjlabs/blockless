package io.github.pjlabs.blockless.continuations;

/** Runs in a separate JVM without exports to check the application's startup error. */
public final class NativeLaunchProbe {
  public static void main(String[] args) {
    try {
      Continuations.run(
          Runnable::run,
          () -> {
            throw new AssertionError("must not run");
          });
      throw new AssertionError("missing export must be rejected");
    } catch (IllegalStateException expected) {
      if (!expected.getMessage().contains("--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED")) {
        throw expected;
      }
      System.out.println("missing export rejected before execution");
    }
  }
}
