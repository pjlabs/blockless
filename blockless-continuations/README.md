# Experimental continuations

Runs synchronous-looking code on a platform executor and releases its worker
while a Blockless supplier is pending. This requires an explicit
`Continuations.run` boundary; ordinary supplier reads outside it still wait.
Uses an internal OpenJDK API, tested on Temurin 21 and 25. No third-party runtime
dependencies, annotations, or agent.

Add `io.github.pjlabs:blockless-continuations` at the same version as core.
For classpath applications, launch with:

```text
--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED
```

On the module path, replace `ALL-UNNAMED` with `blockless.continuations`.
Missing exports fail before the task runs.

```java
var completion = Continuations.run(platformExecutor, () -> {
    var user = Blockless.supplier(() -> loadUser(id)).get();
    process(user);
});
```

Use an application-owned executor that queues work. The caller returns
immediately; do not join `completion` on it. Pending reads suspend the stack
and locals. Completion schedules a resume on the same executor, possibly on a
different thread. Exceptions are thrown at the read; unhandled task failures
complete the returned stage exceptionally.

- Supports `Blockless.get`, `Blockless.supplier(...).get()`, and `Parallel.async(...).get()`.
  Direct I/O and other blocking calls in the task body still block.
- Optional context propagators passed to `run` attach the submission snapshot
  on each resume and restore the executor thread afterward.
- Do not hold monitors, native frames, locks, or thread-affine resources across
  suspension. JVM pinning can prevent yielding.
- Cancellation is unsupported. If the executor rejects a resume, the stage fails
  and suspended cleanup cannot run.

Run `mvn clean verify`. Maven configures the internal API exports for compilation,
Javadoc, and tests. Compatibility with future JDKs is not guaranteed.
