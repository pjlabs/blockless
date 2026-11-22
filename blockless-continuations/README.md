# Experimental continuation suppliers

`Continuations.supplier` runs ordinary blocking code on a virtual thread and
returns a supplier of its cached outcome. Uses public Java 21 APIs. No JVM
exports, opens, agents, preview features, or third-party runtime dependencies.

Add `io.github.pjlabs:blockless-continuations` at the same version as core.

```java
var user = Continuations.supplier(() -> {
    var details = loadDetails(id);
    var permissions = loadPermissions(id);
    return buildUser(details, permissions);
});
var value = user.get();
```

One supplier is enough for sequential blocking work. Start additional suppliers
before reading their results when independent operations should run in parallel.

`supplier` starts eagerly and returns `AsyncSupplier<T>`, which extends
`Supplier<T>`. The callable accepts checked exceptions; `get()` returns its value
or throws its failure after the callable's `finally` blocks finish. Pending
status never causes an exception. Tasks without a value can return `null`.
Optional `onComplete` callbacks run once the outcome is readable, including on
failure.

Pending reads park virtual-thread callers. Ordinary platform callers wait on
their calling thread; releasing that worker requires a virtual-thread task entry.
Avoid waiting on a platform worker needed by the chosen dispatch executor.

The default uses `ForkJoinPool.commonPool()` for dispatch and completion. Use
`Continuations.supplier(executor, task)` to select another executor. Workers only
start the producer and publish its outcome; they never wait for the body.
The producer is constructed at submission and inherits the caller's inheritable
thread-local values and context class loader, even when dispatch starts it later.

Context propagators are optional trailing arguments to either overload. They
capture at submission and attach for the body's lifetime. Context changes survive
blocking calls, and executor workers have their previous context restored.
Ordinary thread-local values require a propagator to cross into the producer.

Initial executor rejection prevents the callable from starting. If completion
dispatch is rejected, the existing producer virtual thread publishes the actual
outcome after cleanup. Executors must throw on rejection and must not silently
discard accepted tasks. Cancellation is unsupported; executor shutdown does not
cancel independently running supplier bodies. Standard JDK virtual-thread pinning
limitations apply.

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
