package io.github.pjlabs.blockless.continuations;

/**
 * Entry point for synchronous-looking code that can suspend on asynchronous suppliers. Use a named
 * or anonymous subclass and enhance its bytecode; JVM-generated lambda classes are not supported.
 */
public abstract class ContinuationTask implements Runnable {
  @Override
  @Suspendable
  public abstract void run();
}
