# Experimental continuations

`Continuations.supplier` runs its callable on a virtual thread, so ordinary
blocking calls can use virtual-thread parking. A native continuation releases
the supplied executor's worker until the body finishes. Pending result reads on ordinary
platform threads throw `IllegalStateException` instead of waiting.
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
var user = Continuations.supplier(platformExecutor, () -> {
    var details = loadDetails(id);
    var permissions = loadPermissions(id);
    return buildUser(details, permissions);
});
```

One supplier is enough for ordinary blocking code. These calls run sequentially
on the body's virtual thread. Start additional suppliers before reading their
results when independent operations should run in parallel.

`supplier` starts eagerly and returns an `AsyncSupplier<T>` (a `Supplier<T>`)
with a cached value or failure. The callable supports checked exceptions and
plain blocking code; no `Blockless.get` wrapper is required. Nested suppliers fork
independent work, and their pending reads park the virtual-thread caller.
Ordinary platform threads can read
completed outcomes; pending reads throw to prevent executor starvation.
Tasks without a value can return `null`.
An optional `onComplete` callback runs once the outcome is readable, including on failure.

Use an application-owned executor that queues work and throws when rejecting it.
It must not silently discard accepted tasks, including during shutdown.
Submitting returns immediately. Suspension preserves
the native waiter's stack. Completion resumes that waiter on the executor to
publish the outcome. The user's body remains on its own virtual thread throughout.
Task failures are thrown at `get()`.

- Context propagators capture at submission and attach for the body's lifetime.
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
