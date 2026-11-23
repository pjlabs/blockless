<p align="center">
  <img src="docs/blockless-logo.png" alt="blockless" width="200">
</p>

<h1 align="center">blockless</h1>

<p align="center"><em>Tame your async — no platform threads were harmed.</em></p>

A tiny Java 21 library for virtual thread concurrency. Two things:

1. **`Blockless.get()`** — wait for async results safely, without entering the future's internals
2. **`Parallel`** — fan out work on virtual threads, collect results, keep your trace context

So you can wait on async results without calling their blocking methods, and your context tags
along for the ride.

Zero dependencies in core. Pluggable context propagation for MDC, gRPC, and OpenTelemetry.

## Quick start

```java
// Wait for a CompletionStage without calling its blocking methods
var result = Blockless.get(CompletableFuture.supplyAsync(() -> "hello"));

// Run a Callable on a virtual thread and get the result
var answer = Blockless.get(() -> expensiveComputation());
```

## Why `Blockless.get()`

`CompletableFuture.join()` parks your thread inside the future's own internals.
If that implementation uses `synchronized` or you're on a platform thread, you
could block or pin.

`Blockless.get()` registers a completion callback and waits on a `CountDownLatch`
through a shared `OutcomeSupplier`. It does not call the future's blocking methods
or create an extra waiting thread. Virtual-thread callers park on the latch;
platform-thread callers block until the outcome is available.

```java
// Wait inside CompletableFuture. May run other pending callbacks on the caller thread.
String result = someFuture.join();

// Wait on a latch released by a completion callback. The wait itself runs no callbacks.
String result = Blockless.get(someFuture);
```

Also works with callables — runs them on a virtual thread and blocks for the result:

```java
var answer = Blockless.get(() -> expensiveComputation());
```

## Experimental: lazy helping

Configure `LazyHelpingExecutor` once as your application's request/task executor:

```java
var requests = LazyHelpingExecutor.newFixedThreadPool(4);
// Install requests as the framework's executor; shut it down with your application.
```

Business code uses the existing APIs:

```java
var user = Blockless.supplier(() -> loadUser(id));
return user.get();
```

Tasks normally start on platform workers. While a worker waits in a pending
`Blockless.get()`, supplier `get()`, or `Parallel.async(...).get()`, it checks its
executor's queue and starts queued tasks on virtual threads. Helping starts only
when a supported wait occurs, hence **lazy helping**. Completion wakes the
original caller, and `get()` returns the real value or throws the task's failure.

The original caller retains its native stack, thread identity, locals and lock
ownership. With no queued work, it waits for a signal. Helped tasks have their own
thread identity; they inherit inheritable thread-local and class-loader state
from their submitter, rather than from the waiting worker.

The worker count limits platform workers, **not total concurrent tasks** or shared
JDK virtual-thread carriers. Arbitrary blocking calls and unmanaged executors do
not help. No context propagators are required; use `PropagatingExecutorService`
when submitting tasks needs explicit context capture. Existing supplier bodies
keep their separate lifetime and are not cancelled with a waiting caller.
Executor termination waits for actual submitted-task cleanup, even after its
Future has been cancelled. `shutdownNow()` interrupts running tasks and cancels
queued Future tasks.

This is an opt-in, experimental executor using public Java 21 APIs. It needs no
agent, bytecode rewriting, preview features or exports/opens. Frameworks must
support configuring their executor; arbitrary existing pools are not intercepted.

Confirmed core tests on these JVMs:

| JVM | Java 21 | Java 25 |
|---|---|---|
| Amazon Corretto | 21.0.13.1+2 | 25.0.5.1+2 |
| Oracle HotSpot | 21.0.12.1+1 | 25.0.5.1+4 |

A local Corretto 21 gRPC comparison measured 368 requests/s for the ordinary
four-worker pool, 939 for lazy helping and 932 for a virtual-thread executor.
All 33,696 measured requests succeeded. These are medians of five samples after
ten seconds warmup, with ten database connections, 10 ms simulated query I/O and
32 concurrent requests. Lazy helping retained four platform request workers;
two shared virtual-thread carriers were observed in both pool modes. These are
snapshots, not a global thread limit. See the
[benchmark fixture](https://github.com/jimbao/blockless/tree/0e765f0e49821572f35223fe506c4d7664ba6ae2/experiments/continuation-entry/alternatives/network).

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

// At most 10 active tasks per map()/toEither() call (sliding window); async() is unaffected
var boundedParallel = parallel.withMaxConcurrency(10);
List<String> names = boundedParallel.map(userIds, id -> fetchName(id));

// Per-task timeout — result fails and worker is interrupted at the deadline
var timedParallel = parallel.withTimeout(Duration.ofSeconds(5));
List<String> names = timedParallel.map(userIds, id -> fetchName(id));

// Combines with withMaxConcurrency
var safeParallel = parallel.withMaxConcurrency(10).withTimeout(Duration.ofSeconds(5));

// Collect results without failing fast — failed tasks return Either.fail()
List<Either<String, Throwable>> results = parallel.toEither(ids, id -> riskyFetch(id));
```

`withMaxConcurrency(n)` is a per-call limit for `map()`, `toEither()`, `asMap()` and
`toEitherMap()`, not a process-wide one: concurrent calls each get their own window, and `async()`
ignores it. For a process-wide cap, guard the call with a shared `Semaphore` in your own code.

Each task's worker is started before its timeout is registered. The timeout budget runs from
registration; time queued for a concurrency slot and any delay before registration do not count.
This is a per-task timeout, not a deadline from submission or for the whole batch. `map()` throws a
wrapped `TimeoutException`, while `toEither()` collects it as `Either.fail()` and continues processing
the other items.

On failure, `map()` interrupts the other active workers without waiting for their cleanup. A task
that ignores interruption may keep running after its result has timed out or the batch has returned.
Callers own resource allocation and cleanup. Submitted code must handle interruption and resources
being closed after the batch returns or throws.

In bounded batches, timed-out calculations retain their slots until they finish. Queued tasks start
only when a slot becomes available; their progress therefore depends on running calculations
responding to cancellation.

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
