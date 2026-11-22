# Experimental continuation suppliers

`Continuations.supplier` runs ordinary blocking code on a virtual thread and
returns a supplier of its cached outcome. Uses public Java 21 APIs. No JVM
exports, opens, agents, preview features, or third-party runtime dependencies.

Add `io.github.pjlabs:blockless-continuations` at the same version as core.

## Write sequential or parallel work

Use one supplier for a sequence of blocking calls:

```java
import io.github.pjlabs.blockless.continuations.Continuations;

var user = Continuations.supplier(() -> {
    var details = loadDetails(id);
    var permissions = loadPermissions(id);
    return buildUser(details, permissions);
});
var value = user.get();
```

Both loads run sequentially on the same logical virtual thread. Keep related
operations in one supplier when they need the same thread-local or resource scope.
The callable can throw checked exceptions and can return `null` when no value is
needed. A plain blocking call inside the body needs no additional Blockless wrapper.

To overlap independent operations, start both suppliers before reading either result:

```java
var details = Continuations.supplier(() -> loadDetails(id));
var permissions = Continuations.supplier(() -> loadPermissions(id));
var user = buildUser(details.get(), permissions.get());
```

Each supplier starts eagerly on its own virtual thread. Repeated `get()` calls read
the cached outcome; they do not run the callable again. The returned
`AsyncSupplier<T>` extends `Supplier<T>` and also exposes `isDone()` and optional
`onComplete` notifications. Ordinary use needs only `get()`.

## How execution works

1. `supplier(...)` captures configured context, constructs a producer virtual thread,
   and submits its start to the dispatch executor. It returns without waiting for the
   body to finish.
2. The executor starts the producer and its worker returns. The callable runs on the
   producer, including its blocking calls and `finally` blocks. The JDK manages the
   virtual thread's continuation and scheduling.
3. If the caller reaches `get()` before the outcome is ready, it waits on the core
   outcome latch. A virtual caller parks, releasing its platform carrier for other
   tasks. Publishing the outcome makes that caller runnable so it can resume inside
   `get()` and continue with the returned value.
4. After the callable's cleanup and context restoration finish, a completion task
   publishes the cached value or failure. Dispatch workers never wait for the body.

The callable gets its own virtual thread. The calling method keeps its existing thread:

| Call to `get()` | Behavior |
|---|---|
| Pending outcome, virtual caller | Parks the caller; its platform carrier can execute other work |
| Pending outcome, platform caller | Waits on that platform thread until the outcome is ready |
| Ready outcome, either caller | Reads the cached value or failure without waiting |

For request handling that must free workers during pending reads, use your
framework's virtual-thread request setting or a virtual-thread task executor such
as `Executors.newVirtualThreadPerTaskExecutor()`. Standard JDK limits still apply,
including monitor pinning on Java 21 and blocking native code.

## Choose dispatch and context

The default dispatch executor is `ForkJoinPool.commonPool()`. To choose another:

```java
var user = Continuations.supplier(dispatchExecutor, () -> loadUser(id));
var value = user.get();
```

The executor handles start and completion tasks; the body always runs on a virtual
thread. Its pool size bounds dispatch workers, not the number of producer threads.
Use the core [Parallel concurrency limit](../README.md#usage) for bounded fan-out.
Custom executors must throw on rejection and must not silently discard accepted
tasks. Do not wait in their only platform worker for their own queued start or
completion tasks: that worker would be unable to run them.

The producer inherits the caller's `InheritableThreadLocal` values and context
class loader at submission, even if dispatch starts it later. Ordinary thread-local
state, including MDC and thread-bound framework context, needs an appropriate
propagator. Propagators are optional trailing arguments to either overload:

```java
import io.github.pjlabs.blockless.context.slf4j.Slf4jMdcContextPropagator;

var user = Continuations.supplier(
    () -> loadUser(id), new Slf4jMdcContextPropagator());
var value = user.get();
```

This example also needs `blockless-context-slf4j` and your SLF4J implementation.
Configured context is captured at submission and attached for the body's lifetime.
Changes survive suspension, and prior context is restored afterward. Nested
suppliers create separate virtual threads; pass their required propagators too.
See the [available propagators](../README.md#available-propagators).

## Outcomes and lifetime

- Pending status never causes an exception. Task runtime exceptions and errors are
  rethrown at `get()`; checked exceptions are wrapped in `RuntimeException`.
- An interrupted pending read restores the caller's interrupt flag and throws
  `RuntimeException` with the interruption as its cause. It does not cancel the
  producer or replace its cached outcome.
- Initial dispatch rejection is read as a failure at `get()` and prevents the body
  from starting. Rejected completion dispatch publishes on the existing producer
  after cleanup, preserving the actual value or failure.
- `onComplete` runs when the outcome is readable, including on failure. It can run
  inline, so its action must be short and nonblocking.
- Supplier bodies run independently. Await their results before closing resources
  they use or ending the application. The API has no cancellation operation, and
  shutting down the dispatch executor does not join or cancel running bodies.

Run `mvn clean verify` without additional JVM arguments.

## Confirmed JVM support

Tested on Linux x86-64 without additional JVM arguments:

| Distribution / VM | Release | Status |
|---|---|---|
| Amazon Corretto / HotSpot | 21.0.13.1+2 | Pass |
| Amazon Corretto / HotSpot | 25.0.5.1+2 | Pass |
| Oracle JDK / HotSpot | 21.0.12.1+1 | Pass |
| Oracle JDK / HotSpot | 25.0.5.1+4 | Pass |

CI also builds with Eclipse Temurin 21 and 25. Other Java 21+ runtimes use the
same public APIs; their specific builds are not confirmed by this table.
