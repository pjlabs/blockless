# Experimental continuations

Runs synchronous-looking code on a platform executor and releases its worker
while a Blockless supplier is pending. This requires an explicit
`Continuations.supplier` boundary; ordinary platform-thread supplier reads
outside it still wait.
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
    var details = Blockless.supplier(() -> loadDetails(id));
    var permissions = Blockless.supplier(() -> loadPermissions(id));
    return buildUser(details.get(), permissions.get());
});
```

`supplier` starts eagerly and returns an `AsyncSupplier<T>` (a `Supplier<T>`)
with a cached value or failure. Its pending `get()` suspends callers inside another
continuation, parks virtual-thread callers, and waits on ordinary platform-thread
callers outside a continuation. Tasks without a value can return `null`.
An optional `onComplete` callback runs once the outcome is readable, including on failure.

Use an application-owned executor that queues work. The caller returns
immediately; reading the result on that caller can still wait. Suspension preserves
the stack and locals. Completion schedules a resume on the same executor, possibly
on a different thread. Task failures are thrown at `get()`.

- Supports `Blockless.get`, `Blockless.supplier(...).get()`, and `Parallel.async(...).get()`.
  Direct I/O and other blocking calls in the task body still block.
- Optional context propagators attach the submission snapshot
  on each resume and restore the executor thread afterward.
- Do not hold monitors, native frames, locks, or thread-affine resources across
  suspension. JVM pinning can prevent yielding.
- Cancellation is unsupported. If the executor rejects a resume, the supplier fails
  and suspended cleanup cannot run.

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
