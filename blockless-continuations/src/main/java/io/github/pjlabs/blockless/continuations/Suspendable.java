package io.github.pjlabs.blockless.continuations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.apache.commons.javaflow.api.ContinuableAnnotation;

/** Marks a method whose stack frame must survive a supplier wait. Requires bytecode enhancement. */
@Documented
@Retention(RetentionPolicy.CLASS)
@Target(ElementType.METHOD)
@ContinuableAnnotation
public @interface Suspendable {}
