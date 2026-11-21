package io.github.pjlabs.blockless.continuations;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import net.tascalate.asmx.ClassWriter;
import net.tascalate.asmx.Opcodes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ContinuationEnhancerTest {
  @TempDir Path directory;

  @Test
  void refusesAnUnenhancedTaskBeforeExecutingIt() throws Exception {
    final var writer = new ClassWriter(0);
    writer.visit(
        Opcodes.V21,
        Opcodes.ACC_PUBLIC,
        "UnenhancedTask",
        null,
        "io/github/pjlabs/blockless/continuations/ContinuationTask",
        null);
    final var constructor = writer.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
    constructor.visitCode();
    constructor.visitVarInsn(Opcodes.ALOAD, 0);
    constructor.visitMethodInsn(
        Opcodes.INVOKESPECIAL,
        "io/github/pjlabs/blockless/continuations/ContinuationTask",
        "<init>",
        "()V",
        false);
    constructor.visitInsn(Opcodes.RETURN);
    constructor.visitMaxs(1, 1);
    constructor.visitEnd();
    final var run = writer.visitMethod(Opcodes.ACC_PUBLIC, "run", "()V", null, null);
    run.visitCode();
    run.visitInsn(Opcodes.RETURN);
    run.visitMaxs(0, 1);
    run.visitEnd();
    writer.visitEnd();
    final var bytes = writer.toByteArray();
    final var loader =
        new ClassLoader(getClass().getClassLoader()) {
          Class<?> taskClass() {
            return defineClass("UnenhancedTask", bytes, 0, bytes.length);
          }
        };
    final var task = (ContinuationTask) loader.taskClass().getConstructor().newInstance();
    final var submissions = new AtomicInteger();
    assertThrows(
        IllegalArgumentException.class,
        () -> Continuations.run(command -> submissions.incrementAndGet(), task));
    assertEquals(0, submissions.get());
  }

  @Test
  void rejectsSynchronizedSuspendableMethodsAtBuildTime() throws Exception {
    final var writer = new ClassWriter(0);
    writer.visit(Opcodes.V21, Opcodes.ACC_PUBLIC, "UnsafeTask", null, "java/lang/Object", null);
    final var method =
        writer.visitMethod(
            Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC | Opcodes.ACC_SYNCHRONIZED,
            "unsafe",
            "()V",
            null,
            null);
    method
        .visitAnnotation("Lio/github/pjlabs/blockless/continuations/Suspendable;", false)
        .visitEnd();
    method.visitCode();
    method.visitInsn(Opcodes.RETURN);
    method.visitMaxs(0, 0);
    method.visitEnd();
    writer.visitEnd();
    final var output = directory.resolve("UnsafeTask.class");
    final var original = writer.toByteArray();
    Files.write(output, original);
    assertThrows(
        IllegalArgumentException.class,
        () -> ContinuationEnhancer.main(new String[] {directory.toString()}));
    assertArrayEquals(original, Files.readAllBytes(output));
  }

  @Test
  void repeatedEnhancementPreservesTheEnhancedBytecode() throws Exception {
    final var name = SampleTask.class.getName().replace('.', '/') + ".class";
    final byte[] original;
    try (final var input = getClass().getClassLoader().getResourceAsStream(name)) {
      original = input.readAllBytes();
    }
    final var output = directory.resolve(name);
    Files.createDirectories(output.getParent());
    Files.write(output, original);
    ContinuationEnhancer.main(new String[] {directory.toString()});
    assertArrayEquals(original, Files.readAllBytes(output));
  }

  static final class SampleTask extends ContinuationTask {
    @Override
    public void run() {}
  }
}
