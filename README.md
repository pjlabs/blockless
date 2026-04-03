<p align="center">
  <img src="docs/blockless-logo.png" alt="blockless" width="200">
</p>

<h1 align="center">blockless</h1>

<p align="center"><em>Tame your async — no platform threads were harmed.</em></p>

A tiny Java 21 library that lets you wait for async results on virtual threads,
so your platform threads stay free and your context tags along for the ride.

## Why

Calling `CompletableFuture.join()` from a platform thread blocks it.
Calling it from a virtual thread is fine — but are you *sure* you're on one?

`Blockless.get()` guarantees the wait happens on a virtual thread, so your
platform threads stay free regardless of where you call from.

```java
// Are you on a platform thread? A virtual thread? Who knows.
String result = someFuture.join();

// Always waits on a virtual thread. Happy dragon noises.
String result = Blockless.get(someFuture);
```

## Quick start

```java
// Wait for a CompletionStage without blocking platform threads
var result = Blockless.get(CompletableFuture.supplyAsync(() -> "hello"));

// Run a Callable on a virtual thread and get the result
var answer = Blockless.get(() -> expensiveComputation());
```

## Parallel execution

Run work concurrently on virtual threads with context propagation built in:

```java
var parallel = Parallel.create(new Slf4jMdcContextPropagator());

// Map in parallel, results stay in order
List<String> names = parallel.map(userIds, id -> fetchName(id));

// Fire off an async task
Supplier<String> data = parallel.async(() -> fetchData());

// Build a map in parallel
Map<String, Profile> profiles = parallel.asMap(userIds, id -> loadProfile(id));
```

## Context propagation

Thread-local context (MDC, gRPC context, OpenTelemetry spans) doesn't survive the
hop to a new thread. Blockless fixes that.

```java
// Wrap a Callable — MDC comes along, get a result back
Callable<String> wrapped = CallableContext.wrap(task, new Slf4jMdcContextPropagator());

// Wrap a Runnable
Runnable wrappedRunnable = RunnableContext.wrap(task, new Slf4jMdcContextPropagator());

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
