# blockless
Simple virtual thread utilities for CompletionStage and Callable that just works!

## Why

Because CompletableFuture.join() almost never does the right thing. 

This library allows you to wait for completion and never block your platform threads.

```java
Blockless.get(CompletableFuture.supplyAsync(() -> "blockless"));
```

