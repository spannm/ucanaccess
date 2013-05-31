package net.ucanaccess.test;


import java.sql.Timestamp;

import net.ucanaccess.converters.TypesMap.AccessType;
import net.ucanaccess.ext.FunctionType;

public class AddFunctionClass {
	@FunctionType(functionName="pluto",argumentTypes={AccessType.TEXT,AccessType.TEXT,AccessType.DATETIME	},returnType=AccessType.TEXT)
	public static String example(String s1,String s2,	Timestamp dt){
		return s1+s2+dt;
	}
	
	@FunctionType(functionName="concat",argumentTypes={AccessType.TEXT,AccessType.TEXT},returnType=AccessType.TEXT)
	public static String concat(String s1,String s2){
		return s1+s2;
	}
	
	

}
