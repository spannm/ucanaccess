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
package net.ucanaccess.converters;

import java.util.HashMap;

import com.healthmarketscience.jackcess.DataType;

public class TypesMap {
	public static enum AccessType{
			BYTE,
			COUNTER,
			CURRENCY, 
			DATETIME,
			DOUBLE,
			GUID,
			INTEGER,
			LONG,
			MEMO, 
			NUMERIC,
			OLE,
			SINGLE,
			TEXT,
			YESNO,
			AUTOINCREMENT,
			COMPLEX
	}
	private static final HashMap<String, String> access2HsqlTypesMap = new HashMap<String, String>();
	private static final HashMap<AccessType, DataType> access2JackcessTypesMap = new HashMap<AccessType, DataType>();
	
	
	private static final HashMap< DataType,String> jackcess2HsqldbTypesMap = new HashMap< DataType,String>();
	
	static {
		
		access2HsqlTypesMap.put(AccessType.BYTE.name(), "SMALLINT");
		access2HsqlTypesMap.put(AccessType.INTEGER.name(), "SMALLINT");
		access2HsqlTypesMap.put(AccessType.LONG.name(), "INTEGER");
		access2HsqlTypesMap.put(AccessType.TEXT.name(), "VARCHAR");
		access2HsqlTypesMap.put(AccessType.OLE.name(), "BLOB");
		access2HsqlTypesMap.put(AccessType.MEMO.name(), "LONGVARCHAR");
		access2HsqlTypesMap.put(AccessType.CURRENCY.name(), "DECIMAL("+DataType.MONEY.getFixedSize()+",4)");
		access2HsqlTypesMap.put(AccessType.GUID.name(), "CHAR(38)");
		access2HsqlTypesMap.put(AccessType.COUNTER.name(), "INTEGER");
		access2HsqlTypesMap.put(AccessType.AUTOINCREMENT.name(), "INTEGER");
		access2HsqlTypesMap.put(AccessType.NUMERIC.name(), "DECIMAL");
		access2HsqlTypesMap.put(AccessType.YESNO.name(), "BOOLEAN");
		access2HsqlTypesMap.put(AccessType.DATETIME.name(), "TIMESTAMP");
		access2HsqlTypesMap.put(AccessType.SINGLE.name(), "FLOAT");
		access2HsqlTypesMap.put(AccessType.COMPLEX.name(), "OBJECT");
				
		access2JackcessTypesMap.put(AccessType.BYTE, DataType.BYTE);
		access2JackcessTypesMap.put(AccessType.INTEGER, DataType.INT);
		access2JackcessTypesMap.put(AccessType.LONG, DataType.LONG); 
		access2JackcessTypesMap.put(AccessType.TEXT, DataType.TEXT);
		access2JackcessTypesMap.put(AccessType.OLE, DataType.OLE);
		access2JackcessTypesMap.put(AccessType.MEMO, DataType.MEMO );
		access2JackcessTypesMap.put(AccessType.CURRENCY, DataType.MONEY);
		access2JackcessTypesMap.put(AccessType.GUID, DataType.GUID);
		access2JackcessTypesMap.put(AccessType.COUNTER, DataType.LONG);
		access2JackcessTypesMap.put(AccessType.AUTOINCREMENT, DataType.LONG);
		access2JackcessTypesMap.put(AccessType.NUMERIC, DataType.NUMERIC);
		access2JackcessTypesMap.put(AccessType.YESNO, DataType.BOOLEAN);
		access2JackcessTypesMap.put(AccessType.DATETIME, DataType.SHORT_DATE_TIME);
		access2JackcessTypesMap.put(AccessType.SINGLE, DataType.FLOAT);
		access2JackcessTypesMap.put(AccessType.DOUBLE, DataType.DOUBLE);
		
		
		jackcess2HsqldbTypesMap.put( DataType.BYTE,"SMALLINT");
		jackcess2HsqldbTypesMap.put( DataType.INT,"SMALLINT");
		jackcess2HsqldbTypesMap.put( DataType.LONG,"INTEGER"); 
		jackcess2HsqldbTypesMap.put( DataType.TEXT,"VARCHAR");
		jackcess2HsqldbTypesMap.put( DataType.BINARY,"BLOB");
		jackcess2HsqldbTypesMap.put( DataType.MEMO,"LONGVARCHAR" );
		jackcess2HsqldbTypesMap.put( DataType.MONEY,"DECIMAL(100,4)");
		jackcess2HsqldbTypesMap.put( DataType.GUID,"CHAR(38)");
		jackcess2HsqldbTypesMap.put( DataType.OLE,"BLOB");
		jackcess2HsqldbTypesMap.put( DataType.NUMERIC,"NUMERIC");
		jackcess2HsqldbTypesMap.put( DataType.BOOLEAN,"BOOLEAN");
		jackcess2HsqldbTypesMap.put(DataType.SHORT_DATE_TIME,"TIMESTAMP");
		jackcess2HsqldbTypesMap.put( DataType.FLOAT,"FLOAT");
		jackcess2HsqldbTypesMap.put( DataType.DOUBLE,"DOUBLE");
		jackcess2HsqldbTypesMap.put( DataType.COMPLEX_TYPE,"OBJECT");
		jackcess2HsqldbTypesMap.put( DataType.UNKNOWN_11,"BLOB");
		jackcess2HsqldbTypesMap.put( DataType.UNKNOWN_0D,"BLOB");
		jackcess2HsqldbTypesMap.put( DataType.UNSUPPORTED_FIXEDLEN,"BLOB");
		jackcess2HsqldbTypesMap.put( DataType.UNSUPPORTED_VARLEN,"BLOB");
		
	}
	
	public static HashMap<String, String> getAccess2HsqlTypesMap() {
		return access2HsqlTypesMap;
	}
	public static String map2hsqldb(DataType type) {
			if (jackcess2HsqldbTypesMap.containsKey(type)) {
			return jackcess2HsqldbTypesMap.get(type);
		}
		return type.name();
	}
	
	public static DataType map2Jackcess(AccessType type) {
			return access2JackcessTypesMap.get(type);
	}
	
	
}
