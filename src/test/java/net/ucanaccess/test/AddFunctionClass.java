/*
Copyright (c) 2012 Marco Amadei.

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307
USA

You can contact Marco Amadei at amadei.mar@gmail.com.

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
