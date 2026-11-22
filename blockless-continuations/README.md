# Experimental continuations

`Continuations.supplier` runs its callable on a virtual thread, so ordinary
blocking calls can use virtual-thread parking. A native continuation releases
the dispatch executor's worker until the body finishes. A pending `get()` waits
for the actual value or task failure; pending status never causes an exception.
Uses an internal OpenJDK API. No third-party runtime dependencies, annotations,
or agent.

Add `io.github.pjlabs:blockless-continuations` at the same version as core.
For classpath applications, launch with:

```text
--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED
```

On the module path, replace `ALL-UNNAMED` with `blockless.continuations`.
Missing exports fail before the task runs.

```java
var user = Continuations.supplier(() -> {
    var details = loadDetails(id);
    var permissions = loadPermissions(id);
    return buildUser(details, permissions);
});
var value = user.get();
```

One supplier is enough for ordinary blocking code. These calls run sequentially
on the body's virtual thread. Start additional suppliers before reading their
results when independent operations should run in parallel.

`supplier` starts eagerly and returns an `AsyncSupplier<T>` (a `Supplier<T>`)
with a cached value or failure. The callable supports checked exceptions and
plain blocking code; no `Blockless.get` wrapper is required. Nested suppliers fork
independent work. Pending reads park virtual-thread callers; completion makes
them runnable again. An ordinary platform caller waits on its calling thread.
Freeing that caller's worker requires a virtual-thread task entry. In particular,
do not wait on a platform worker if the chosen dispatch executor needs that same
worker to start or complete the supplier.
Tasks without a value can return `null`.
An optional `onComplete` callback runs once the outcome is readable, including on failure.

The default uses `ForkJoinPool.commonPool()` for dispatch and completion; the body
always runs on a virtual thread. Java does not expose a general current executor.
Use `Continuations.supplier(executor, task)` to choose one explicitly. A custom
executor must throw on rejection and must not silently discard accepted tasks,
including during shutdown.
Submitting returns immediately. Suspension preserves
the native waiter's stack. Completion resumes that waiter on the executor to
publish the outcome. The user's body remains on its own virtual thread throughout.
Task failures are thrown at `get()`.

- Context propagators are optional trailing arguments to either overload. They
  capture at submission and attach for the body's lifetime.
  Changes, including clearing context, survive blocking calls. Executor workers
  have their previous context restored afterward.
- Standard virtual-thread limitations apply, including monitor pinning on Java 21
  and blocking native code.
- Initial executor rejection prevents the body from starting. Rejected completion
  resumes use a fallback virtual thread to publish the actual value or failure.
  The body is never restarted; its `finally` blocks finish before publication.
- Cancellation is unsupported; executor shutdown does not cancel a running body.

Run `mvn clean verify`. Maven configures the internal API exports for compilation,
Javadoc, and tests. Compatibility with future JDKs is not guaranteed.

## JVM support

Confirmed on Linux x86-64 with the required export flag:

| Distribution / VM | Tested release or build | Status |
|---|---|---|
| Eclipse Temurin / HotSpot | 21.0.13.1+2 | Pass |
| Eclipse Temurin / HotSpot | 25.0.5.1+2 | Pass |
| Oracle JDK / HotSpot | 21.0.12.1+1 (archived) | Pass |
| Oracle JDK / HotSpot | 25.0.5.1+4 | Pass |
| Amazon Corretto / HotSpot | 21.0.13.1.2 | Pass |
| Amazon Corretto / HotSpot | 25.0.5.1.2 | Pass |
| IBM Semeru / OpenJ9 0.63.0 | 21.0.13.0 | Unsupported |

OpenJ9 lost task exceptions and terminated with a VM assertion on virtual-thread
entry. Ordinary Blockless and public virtual-thread usage passed on that JVM.
Untested builds and platforms are not covered by this table.
