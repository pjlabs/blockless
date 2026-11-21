# Experimental native continuations

**Experimental:** this optional module uses Tascalate JavaFlow's experimental
`net.tascalate.javaflow.api-jvmc` backend and an internal OpenJDK API. Its API and
JDK compatibility may change. Core remains dependency-free and does not require
JVM exports unless you opt in to continuations.

A task can use ordinary synchronous Java, including lambdas and unannotated helpers:

```java
import io.github.pjlabs.blockless.Blockless;
import io.github.pjlabs.blockless.Parallel;
import io.github.pjlabs.blockless.continuations.Continuations;

// Keep the executor alive for the application's lifetime.
var completion = Continuations.run(platformExecutor, () -> {
    var user = loadUser(id);
    var parallel = Parallel.create();
    var orders = parallel.async(() -> blockingLoadOrders(user));
    var preferences = parallel.async(() -> blockingLoadPreferences(user));
    process(user, orders.get(), preferences.get());
});
// Continue handling other work. Do not join completion on the submitting thread.

static User loadUser(String id) {
    return Blockless.supplier(() -> blockingLoadUser(id)).get();
}
```

An unfinished supplier read suspends the native continuation, preserving locals,
operand stacks, and nested helper frames. The platform worker returns to its
executor. Completion schedules a single-shot resume on the same executor,
which may use another thread. Failures are thrown at the original read, and
ordinary `catch` and `finally` blocks work.

Completed suppliers return inline. Virtual-thread reads use their ordinary
parking behavior. `Blockless.get(Callable)`, `Blockless.get(CompletionStage)`, and
suppliers returned by `Parallel.async` use the same runtime wait hook.

## Opt in

Add `io.github.pjlabs:blockless-continuations` using the same version as core.
For a classpath application, start Java with:

```text
--add-exports=java.base/jdk.internal.vm=ALL-UNNAMED
```

For an application running JavaFlow as a named module, export to its module:

```text
--add-exports=java.base/jdk.internal.vm=net.tascalate.javaflow.api
```

Set the flag in each runtime that executes continuations, including IDE launch
configurations and test JVMs. `Continuations.run` checks the export before
submitting work and reports the required flag if missing. No preview features
are required on Java 21.

**No annotations, bytecode enhancer, or Java agent are needed.** When migrating
from the bytecode backend, remove its enhancement plugin/agent and recompile
from clean sources. Do not include both `net.tascalate.javaflow.api` and
`net.tascalate.javaflow.api-jvmc`: they provide the same API packages.

This repository configures the runtime export for the continuation module's
Surefire test JVM. Run all tests from the repository root:

```sh
mvn clean verify
```

## Execution contract

- Opt in through `Continuations.run(executor, Runnable, propagators...)`. This
  establishes the continuation boundary; it cannot migrate an existing arbitrary
  platform-thread stack. Ordinary calls outside that boundary still wait.
- Suspension is limited to the runtime wait paths of `Blockless.supplier`, direct
  `Blockless.get` calls, and `Parallel.async` suppliers. Arbitrary suppliers,
  `Parallel.map`, and direct blocking I/O in the task body retain their usual
  behavior. Put blocking work inside a Blockless supplier or parallel async task.
- Use an executor that queues work. Direct executors and caller-runs rejection
  policies may execute on the submitting or completing thread. Resumption does
  not promise the original thread.
- Do not hold monitors, native frames, locks, or thread-affine resources across
  suspension. JVM pinning can prevent native continuation yielding; the backend
  may fail the task. In particular, using a recent JDK does not make arbitrary
  native continuation yields safe inside monitors.
- Pass context propagators to `Continuations.run` to attach the submission
  snapshot on every resume and restore the executor thread's previous context.
  Configure `Parallel` separately if its workers need the same context.
- The returned completion stage reports the whole task's outcome. It does not
  cancel the continuation or its workers. Executor rejection completes the stage
  exceptionally; suspended cleanup cannot run if the executor refuses a resume.
- Virtual-thread-aware I/O is still required to release a worker's carrier.
  Arbitrary native blocking work may occupy that carrier.

## Runtime design

Core's `ResultSupplier.get()` invokes a scoped `SupplierWaitSupport` handler
only for unfinished platform-thread reads. The optional module installs the
handler around native continuation execution and restores the previous handler
on exit. With no handler installed, core keeps its normal behavior.

The handler yields the native stack and supplies its `AsyncSupplier` readiness
signal to the scheduler. Callbacks are registered after the stack has unwound.
An atomic signal counter serializes resumes and handles inline completion
notification without recursive stack growth.

See the [Tascalate JavaFlow 2.9.0 release notes](https://github.com/vsilaev/tascalate-javaflow/releases/tag/2.9.0)
and [native-backend setup](https://github.com/vsilaev/tascalate-javaflow#important-notice-for-release-290).
The state-machine alternative is described in
[Kotlin's coroutine specification](https://kotlinlang.org/spec/asynchronous-programming-with-coroutines.html);
this backend instead lets the JVM preserve the native continuation stack.
