package io.github.pjlabs.blockless;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

final class ResultSupplier<T> implements AsyncSupplier<T> {
  private final Supplier<T> value;
  private final CompletableFuture<?> completion;

  ResultSupplier(Supplier<T> value, CompletableFuture<?> completion) {
    this.value = value;
    this.completion = completion;
  }

  @Override
  public T get() {
    return value.get();
  }

  @Override
  public boolean isDone() {
    return completion.isDone();
  }

  @Override
  public void onComplete(Runnable action) {
    completion.whenComplete((ignored, failure) -> action.run());
  }
}
