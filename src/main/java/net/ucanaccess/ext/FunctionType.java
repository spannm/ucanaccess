package net.ucanaccess.ext;

import net.ucanaccess.converters.TypesMap.AccessType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface FunctionType {

    AccessType[] argumentTypes();

    String functionName();

    boolean namingConflict() default false;

    AccessType returnType();

}
