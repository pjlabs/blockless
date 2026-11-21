package io.github.pjlabs.blockless.continuations;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import net.tascalate.asmx.ClassReader;
import net.tascalate.asmx.ClassWriter;
import net.tascalate.asmx.Opcodes;
import net.tascalate.asmx.tree.AnnotationNode;
import net.tascalate.asmx.tree.ClassNode;
import net.tascalate.asmx.tree.MethodInsnNode;
import org.apache.commons.javaflow.providers.core.ContinuableClassTransformationFactory;
import org.apache.commons.javaflow.spi.ClasspathResourceLoader;

/**
 * Build-time enhancer. Run with the application's dependencies on the context class loader and one
 * or more compiled class directories as arguments. Repeated enhancement is safe.
 */
public final class ContinuationEnhancer {
  private static final String PACKAGE = "io/github/pjlabs/blockless/continuations/";
  private static final String RUNTIME = PACKAGE + "Continuations";
  private static final String ANNOTATION = "L" + PACKAGE + "Suspendable;";
  private static final String PROCESSED = "Lorg/apache/commons/javaflow/core/Skip;";

  private ContinuationEnhancer() {}

  public static void main(String[] directories) throws IOException {
    if (directories.length == 0) {
      throw new IllegalArgumentException("Supply compiled class directories to enhance");
    }
    final var sources = new LinkedHashMap<String, Path>();
    final var prepared = new LinkedHashMap<String, byte[]>();
    for (final var directory : directories) {
      final var root = Path.of(directory);
      try (final var paths = Files.walk(root)) {
        for (final var path : paths.filter(p -> p.toString().endsWith(".class")).toList()) {
          final var name = root.relativize(path).toString().replace('\\', '/');
          sources.put(name, path);
          prepared.put(name, prepare(Files.readAllBytes(path)));
        }
      }
    }
    final var resources =
        new PreparedResources(Thread.currentThread().getContextClassLoader(), prepared);
    final var transformer =
        new ContinuableClassTransformationFactory().createTransformer(resources);
    try {
      // Resolve every class against the same prepared bytecode, independent of directory order.
      final var enhanced = new LinkedHashMap<String, byte[]>();
      for (final var entry : prepared.entrySet()) {
        final var transformed = transformer.transform(entry.getValue());
        enhanced.put(entry.getKey(), transformed == null ? entry.getValue() : transformed);
      }
      for (final var entry : enhanced.entrySet()) {
        Files.write(sources.get(entry.getKey()), entry.getValue());
      }
    } finally {
      transformer.release();
    }
  }

  private static byte[] prepare(byte[] original) {
    final var type = new ClassNode(Opcodes.ASM9);
    new ClassReader(original).accept(type, 0);
    if (hasAnnotation(type.visibleAnnotations, PROCESSED)) {
      return original;
    }
    for (final var method : type.methods) {
      final var taskEntry =
          (PACKAGE + "ContinuationTask").equals(type.superName)
              && method.name.equals("run")
              && method.desc.equals("()V");
      if (!taskEntry && !hasAnnotation(method.invisibleAnnotations, ANNOTATION)) {
        continue;
      }
      if (taskEntry && !hasAnnotation(method.invisibleAnnotations, ANNOTATION)) {
        if (method.invisibleAnnotations == null) {
          method.invisibleAnnotations = new ArrayList<>();
        }
        method.invisibleAnnotations.add(new AnnotationNode(ANNOTATION));
      }
      if ((method.access & Opcodes.ACC_SYNCHRONIZED) != 0) {
        throw new IllegalArgumentException(
            "Suspendable methods cannot hold monitors: " + type.name);
      }
      for (final var instruction : method.instructions) {
        if (instruction.getOpcode() == Opcodes.MONITORENTER) {
          throw new IllegalArgumentException(
              "Suspendable methods cannot hold monitors: " + type.name);
        }
        // The runtime's own annotated methods are suspension implementations, not call sites.
        if (type.name.equals(RUNTIME) || !(instruction instanceof MethodInsnNode call)) {
          continue;
        }
        if ((call.owner.equals("java/util/function/Supplier")
                || call.owner.equals("io/github/pjlabs/blockless/AsyncSupplier"))
            && call.name.equals("get")
            && call.desc.equals("()Ljava/lang/Object;")) {
          call.setOpcode(Opcodes.INVOKESTATIC);
          call.owner = RUNTIME;
          call.name = "await";
          call.desc = "(Ljava/util/function/Supplier;)Ljava/lang/Object;";
          call.itf = false;
        } else if (call.owner.equals("io/github/pjlabs/blockless/Blockless")
            && call.name.equals("get")) {
          call.owner = RUNTIME;
        }
      }
    }
    final var writer = new ClassWriter(0);
    type.accept(writer);
    return writer.toByteArray();
  }

  private static boolean hasAnnotation(List<AnnotationNode> annotations, String descriptor) {
    return annotations != null && annotations.stream().anyMatch(a -> a.desc.equals(descriptor));
  }

  private static final class PreparedResources extends ClasspathResourceLoader {
    private final Map<String, byte[]> prepared;

    private PreparedResources(ClassLoader loader, Map<String, byte[]> prepared) {
      super(loader);
      this.prepared = prepared;
    }

    @Override
    public boolean hasResource(String name) {
      return prepared.containsKey(name) || super.hasResource(name);
    }

    @Override
    public InputStream getResourceAsStream(String name) throws IOException {
      final var bytes = prepared.get(name);
      return bytes == null ? super.getResourceAsStream(name) : new ByteArrayInputStream(bytes);
    }
  }
}
