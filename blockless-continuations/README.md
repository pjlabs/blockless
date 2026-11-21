# Platform-thread continuations

This optional module lets an executor thread submit blocking I/O to a virtual
thread and resume at the supplier read when the result is ready. The body reads
like synchronous Java:

```java
import io.github.pjlabs.blockless.Blockless;
import io.github.pjlabs.blockless.Parallel;
import io.github.pjlabs.blockless.continuations.ContinuationTask;
import io.github.pjlabs.blockless.continuations.Continuations;

// Keep the executor alive for the application's lifetime.
var completion = Continuations.run(platformExecutor, new ContinuationTask() {
    @Override
    public void run() {
        var user = Blockless.supplier(() -> blockingLoadUser(id)).get();

        var parallel = Parallel.create();
        var orders = parallel.async(() -> blockingLoadOrders(user));
        var preferences = parallel.async(() -> blockingLoadPreferences(user));

        process(user, orders.get(), preferences.get());
    }
});
// The submitting thread can handle other work. Do not join completion here.
```

On a platform executor, an unfinished `get()` saves the live locals, operand stack,
and resume location, then unwinds to the executor. Completion queues a resume on
the same executor, which may choose a different thread. No thread waits for the
supplier result. A completed supplier returns inline. On a virtual executor,
the ordinary supplier wait parks the virtual thread instead.

`Blockless.get(Callable)` and `Blockless.get(CompletionStage)` are also suspension
points. Failures are thrown at the original read, so ordinary `catch` and `finally`
blocks work. The completion stage returned by `Continuations.run` reports the
whole task's success or failure.

## Build-time enhancement is required

Add `io.github.pjlabs:blockless-continuations` using the same version as core.
Then run `ContinuationEnhancer` after compilation with your application's
dependencies on its classpath:

```xml
<plugin>
    <groupId>org.codehaus.mojo</groupId>
    <artifactId>exec-maven-plugin</artifactId>
    <version>3.5.0</version>
    <executions>
        <execution>
            <id>enhance-continuations</id>
            <phase>process-classes</phase>
            <goals><goal>java</goal></goals>
            <configuration>
                <mainClass>io.github.pjlabs.blockless.continuations.ContinuationEnhancer</mainClass>
                <arguments>
                    <argument>${project.build.outputDirectory}</argument>
                </arguments>
            </configuration>
        </execution>
    </executions>
</plugin>
```

For tests, add the same execution at `process-test-classes`, use
`${project.build.testOutputDirectory}`, and set `classpathScope` to `test`.
An unenhanced entry task is rejected before it runs. Repeated enhancement is safe.
Use `mvn clean verify` when changing continuation methods.

The enhancer recognizes the `run()` method of direct `ContinuationTask` subclasses.
Mark each helper method between that entry and a supplier read with `@Suspendable`
and enhance the classes containing those methods too:

```java
@Suspendable
static String loadName(String id) {
    var prefix = "User: ";
    return prefix + Blockless.supplier(() -> blockingLoadName(id)).get();
}
```

## Scope

- This is an opt-in bytecode implementation, not a migration of an arbitrary
  existing platform-thread stack. Only enhanced frames can suspend.
- Use named or anonymous `ContinuationTask` subclasses. JVM-generated task
  lambdas and unenhanced helper frames are not supported.
- Suspension points are reads of suppliers returned by `Blockless.supplier`,
  `Parallel.async`, and direct `Blockless.get` calls in enhanced methods.
  `Parallel.map`, arbitrary suppliers, blocking calls outside these boundaries,
  and reflection into suspending methods are outside this contract.
- The enhancer rejects monitors in suspendable methods. Do not hold locks or
  thread-affine resources across a wait: a resume can use another thread.
- Pass context propagators to `Continuations.run(executor, task, propagators)`
  to attach the submission snapshot on every resume. Configure `Parallel` with
  the same propagators when its worker tasks need that context.
- Use an executor that queues work. A direct executor or caller-runs rejection
  policy can execute the continuation on the submitting or completing thread.
  Executor rejection completes the task exceptionally; suspended cleanup cannot
  run if the executor refuses the resume. The returned stage does not support
  cancellation of the task or its workers.
- Blocking work still needs virtual-thread-aware I/O to release its carrier.
  Moving arbitrary native blocking code to a virtual thread does not guarantee
  that its carrier is released.

## Implementation and references

The core module exposes supplier readiness through `AsyncSupplier` without any
external dependencies. The optional module rewrites supplier reads into an await
boundary, then uses [Tascalate JavaFlow](https://github.com/vsilaev/tascalate-javaflow)
2.9.0 to preserve and restore execution state. It uses the regular bytecode API,
not the experimental internal-JVM continuation API; no preview features or
`--add-exports` flags are required on Java 21.

The state-machine model follows
[Kotlin's coroutine specification](https://kotlinlang.org/spec/asynchronous-programming-with-coroutines.html).
Completion callbacks are registered after the stack has unwound. An atomic
signal counter serializes resumes and handles completion during registration
without recursive stack growth, including with a direct executor.

Run the implementation and integration tests from the repository root:

```sh
mvn clean verify
```
