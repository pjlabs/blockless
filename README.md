<p align="center">
  <img src="docs/blockless-logo.png" alt="blockless" width="200">
</p>

<h1 align="center">blockless</h1>

<p align="center"><em>Tame your async — no platform threads were harmed.</em></p>

A tiny Java 21 library for virtual thread concurrency. Two things:

1. **`Blockless.get()`** — wait for async results safely, without entering the future's internals
2. **`Parallel`** — fan out work on virtual threads, collect results, keep your trace context

So your platform threads stay free and your context tags along for the ride.

Zero dependencies in core. Pluggable context propagation for MDC, gRPC, and OpenTelemetry.

## Quick start

```java
// Wait for a CompletionStage without blocking platform threads
var result = Blockless.get(CompletableFuture.supplyAsync(() -> "hello"));

// Run a Callable on a virtual thread and get the result
var answer = Blockless.get(() -> expensiveComputation());
```

## Why `Blockless.get()`

`CompletableFuture.join()` parks your thread inside the future's own internals.
If that implementation uses `synchronized` or you're on a platform thread, you
could block or pin.

`Blockless.get()` never enters the `CompletionStage` internals. It waits on a
virtual thread via a `CountDownLatch` — your thread is never at risk, regardless
of what the stage does internally.

```java
// Parks inside CompletableFuture internals — you're at the mercy of the implementation.
String result = someFuture.join();

// Waits on a virtual thread + latch. Your thread never touches the future. 🐉
String result = Blockless.get(someFuture);
```

**Defensive coding:** `Blockless.get()` is safe whether your calling thread is a virtual thread or a platform thread. If a refactor, config change, or library upgrade silently changes what thread your code runs on, `.join()` could start blocking platform threads with no warning. `Blockless.get()` stays safe regardless — you don't have to know or care what thread you're on.

Also works with callables — runs them on a virtual thread and blocks for the result:

```java
var answer = Blockless.get(() -> expensiveComputation());
```

## Why `Parallel`

Services often need to fan out N calls and collect the results. The typical
pattern looks like this:

```java
// Before: manual executor + supplyAsync + semaphore + join
var executor = Executors.newVirtualThreadPerTaskExecutor();
var semaphore = new Semaphore(10);
var futures = ids.stream()
    .map(id -> CompletableFuture.supplyAsync(() -> {
        semaphore.acquireUninterruptibly();
        try { return fetchById(id); }
        finally { semaphore.release(); }
    }, executor))
    .toList();
var results = futures.stream().map(CompletableFuture::join).toList();
```

With blockless:

```java
// After: one line, same behavior
var results = parallel.withMaxConcurrency(10).map(ids, this::fetchById);
```

Each task runs on its own virtual thread. Results stay in input order.
MDC and trace context survive the hop.

### Usage

```java
var parallel = Parallel.create(new Slf4jMdcContextPropagator());

// Map in parallel, results stay in order
List<String> names = parallel.map(userIds, id -> fetchName(id));

// Fire off an async task, get result later
Supplier<String> data = parallel.async(() -> fetchData());

// Build a map in parallel
Map<String, Profile> profiles = parallel.asMap(userIds, id -> loadProfile(id));

// Limit concurrent tasks (extras park until a permit frees up)
var boundedParallel = parallel.withMaxConcurrency(10);
List<String> names = boundedParallel.map(userIds, id -> fetchName(id));

// Per-task timeout — thread is interrupted if task exceeds the deadline
var timedParallel = parallel.withTimeout(Duration.ofSeconds(5));
List<String> names = timedParallel.map(userIds, id -> fetchName(id));

// Combines with withMaxConcurrency
var safeParallel = parallel.withMaxConcurrency(10).withTimeout(Duration.ofSeconds(5));

// Collect results without failing fast — failed tasks return Either.fail()
List<Either<String, Throwable>> results = parallel.toEither(ids, id -> riskyFetch(id));
```

## Context propagation

When you hop to a new thread, thread-local context (MDC trace IDs, gRPC context,
OpenTelemetry spans) is lost. Logs become uncorrelated. Traces break.

`Parallel` handles this automatically — context is captured when you call `map()`/`async()`
and restored in each worker thread. You choose which propagators to wire up:

```java
var parallel = Parallel.create(
    new Slf4jMdcContextPropagator(),
    new GrpcContextPropagator()
);
```

For lower-level use, you can wrap individual tasks:

```java
// Wrap a Callable — MDC comes along
Callable<String> wrapped = CallableContext.wrap(task, new Slf4jMdcContextPropagator());

// Wrap a Runnable
Runnable wrapped = RunnableContext.wrap(task, new Slf4jMdcContextPropagator());

// Wrap an entire ExecutorService
ExecutorService executor = PropagatingExecutorService.wrap(
    Executors.newVirtualThreadPerTaskExecutor(),
    new Slf4jMdcContextPropagator(),
    new GrpcContextPropagator()
);
```

### Available propagators

| Module | Propagates |
|---|---|
| `blockless-context-slf4j` | SLF4J MDC |
| `blockless-context-grpc` | gRPC `Context` |
| `blockless-context-opentelemetry` | OpenTelemetry `Context` |

Framework dependencies are `provided` scope — blockless uses whatever version
your service already has. No version conflicts.

## Modules

| Module | What it does |
|---|---|
| `blockless` | Core utilities: `Blockless`, `Parallel`, context SPI |
| `blockless-context-slf4j` | SLF4J MDC propagation |
| `blockless-context-grpc` | gRPC context propagation |
| `blockless-context-opentelemetry` | OpenTelemetry context propagation |
| `blockless-tests` | Integration tests |

## Requirements

- Java 21+

## License

Apache 2.0
