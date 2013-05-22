package net.ucanaccess.ext;
import java.lang.annotation.*;

import net.ucanaccess.converters.TypesMap.AccessType;
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface FunctionType {
	AccessType[] argumentTypes() ;
	String functionName();
	boolean namingConflict() default  false;
	AccessType returnType(); 
	
}
