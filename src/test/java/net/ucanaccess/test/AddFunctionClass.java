/*
Copyright (c) 2012 Marco Amadei.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
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

