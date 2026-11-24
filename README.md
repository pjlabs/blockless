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

## Experimental: lazy helping

When every worker in a platform pool is waiting for I/O, new tasks remain queued.
`LazyHelpingExecutor` lets a waiting worker keep that queue moving by starting
queued tasks on virtual threads. Business code still reads synchronously.

The design has three steps:

1. A submitted task, such as a request handler, normally starts on a platform worker.
2. At a supported pending `get()`, that worker keeps the original caller's stack
   and dispatches queued tasks onto virtual threads.
3. When its own result arrives, `get()` returns and the original task continues.
   Already-started helped tasks continue independently.

The platform pool has a fixed worker count. Helped tasks use virtual threads that
share the JDK's native carrier threads; they do not need a native thread per task.

### Usage

Create the executor once and submit the outer task containing the wait:

```java
var requests = LazyHelpingExecutor.newFixedThreadPool(4);
var result = requests.submit(() -> {
    var user = Blockless.supplier(() -> loadUser(id));
    return user.get();
});
// Shut down requests with your application.
```

Supported waits are `Blockless.get(...)`, `Blockless.supplier(...).get()` and
`Parallel.async(...).get()`. Only the outer task containing that wait needs to
run on this executor. Installing it as a framework's request executor is optional.

The submitting thread is outside this boundary. Calling `result.get()` there is
an ordinary Future wait. To keep a platform submitting thread available, hand off
the task and return instead of waiting for the outer Future.

Use `LazyHelpingExecutor.newFixedThreadPool(4, maxHelped)` with a positive
`maxHelped` to cap concurrent helped tasks across the executor. Size the cap to
the downstream resource, such as a database connection pool, allowing room for
platform workers' tasks. When the cap is reached, work stays queued until a
helped task exits or a platform worker takes it. The
`(workers, maxHelped, factory)` overload also accepts a custom platform thread
factory. Overloads without `maxHelped` leave helping unbounded.

### Five requests with one platform worker

Use `LazyHelpingExecutor.newFixedThreadPool(1)`. Assume all five requests execute
the task shown above, and request 1's I/O stays pending while requests 2–5 arrive.

Platform worker `P` starts request 1. Its supplier starts an I/O virtual thread.
At `get()`, `P` enters the helping loop while retaining request 1's stack.
It takes queued requests 2–5 and starts each on a separate virtual thread.
Each helped thread runs an entire outer request, whose supplier starts its own
I/O virtual thread.

| Request | Thread running request code | While its supplier is pending |
|---|---|---|
| 1 | Platform worker `P` | Helps dispatch queued requests |
| 2 | Virtual thread `V2` | Parks |
| 3 | Virtual thread `V3` | Parks |
| 4 | Virtual thread `V4` | Parks |
| 5 | Virtual thread `V5` | Parks |

Once the queue is empty, `P` waits for more queued work or request 1's result.
If request 3's I/O finishes first, the JDK resumes `V3` after its `get()` and
request 3 completes independently of `P`.

When request 1's I/O finishes, `P` exits its helping loop and its `get()` returns.
It does not wait for requests 2–5 or for the queue to empty. After request 1
finishes, `P` can take another task from the normal queue.

If all five I/O bodies are pending at once, there is one platform request
worker, four started virtual caller threads and five virtual supplier-body
threads, plus the shared native carriers. `P` dispatches all five requests;
only request 1's business code runs directly on `P`.

### Queues and task ownership

Ordinary fixed-pool scheduling decides what is queued: the executor creates
platform workers up to its configured count, then offers submissions to its
shared queue. An idle worker may take a submission immediately. A worker inside
a helping wait can also take queued tasks and launch them on virtual threads.
Only this executor's queue is visible to helping.

The helper executor uses `Executors.newThreadPerTaskExecutor` with a factory that
returns virtual threads. It tracks started tasks until they actually exit. The
JDK schedules and resumes those threads; they do not depend on their launching
platform worker continuing to help. Remaining queued tasks are taken by normal
platform workers or another helping worker.

### Scope and shutdown

The original caller retains its native stack, thread identity, locals and lock
ownership. Helped tasks have their own thread identity and inherit inheritable
thread-local and class-loader state from their submitter. No context propagators
are required; use `PropagatingExecutorService` for explicit context capture.

The worker count limits platform workers. Shared JDK carriers are managed by
the JVM. Other blocking calls and unmanaged executors do not trigger helping.
The original platform worker waits for a signal when it has no queued work to
launch or the helping cap has been reached. Its stack stays on that worker
throughout the wait.

`shutdown()` rejects new submissions, processes accepted queued tasks and waits
for running outer tasks, including helped tasks, to exit. A cancelled Future
can be done before its task's `finally` finishes, so termination tracks actual
task exit. `shutdownNow()` interrupts running tasks and cancels queued Future
tasks. Supplier bodies retain their existing separate lifetime and are not
cancelled with a waiting caller.

This opt-in, experimental executor uses public Java 21 APIs. It needs no agent,
bytecode rewriting, preview features or exports/opens.

### JVM support and measurements

On Java 21, helped tasks that block inside `synchronized` code pin a carrier
thread. For workloads with this blocking pattern, use Java 24 or later, which
removes pinning due to synchronization through [JEP 491](https://openjdk.org/jeps/491).

Confirmed core tests on these JVMs:

| JVM | Java 21 | Java 25 |
|---|---|---|
| Amazon Corretto | 21.0.13.1+2 | 25.0.5.1+2 |
| Oracle HotSpot | 21.0.12.1+1 | 25.0.5.1+4 |

A local Corretto 21 gRPC comparison measured 368 requests/s for the ordinary
four-worker pool, 939 for lazy helping and 932 for a virtual-thread executor.
All 33,696 measured requests succeeded. These are medians of five samples after
ten seconds warmup, with ten database connections, 10 ms simulated query I/O and
32 concurrent requests. Lazy helping retained four platform request workers.
Two shared virtual-thread carriers were observed in both pool modes during
the measurement.

<!-- TODO Add a repository-relative link when the benchmark fixture is available here. -->

### About the name

"Lazy helping" is a descriptive working name chosen for this implementation,
not a standard Java feature. "Lazy" means it activates during a supported wait;
"helping" means the waiting worker dispatches queued tasks. Related task helping
exists in the JDK, for example
[`ForkJoinTask.helpQuiesce()`](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/util/concurrent/ForkJoinTask.html#helpQuiesce()).
That method executes tasks; this executor dispatches them onto virtual threads.
