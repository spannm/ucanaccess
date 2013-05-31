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
			YESNO
	}
	private static final HashMap<String, String> access2HsqlTypesMap = new HashMap<String, String>();
	private static final HashMap<AccessType, DataType> access2JackcessTypesMap = new HashMap<AccessType, DataType>();
	
	
	private static final HashMap< DataType,String> jackcess2HsqldbTypesMap = new HashMap< DataType,String>();
	
	static {
		
		access2HsqlTypesMap.put(AccessType.BYTE.name(), "TINYINT");
		access2HsqlTypesMap.put(AccessType.INTEGER.name(), "SMALLINT");
		access2HsqlTypesMap.put(AccessType.LONG.name(), "INTEGER");
		access2HsqlTypesMap.put(AccessType.TEXT.name(), "VARCHAR");
		access2HsqlTypesMap.put(AccessType.OLE.name(), "BLOB");
		access2HsqlTypesMap.put(AccessType.MEMO.name(), "LONGVARCHAR");
		access2HsqlTypesMap.put(AccessType.CURRENCY.name(), "DECIMAL("+DataType.MONEY.getFixedSize()+",4)");
		access2HsqlTypesMap.put(AccessType.GUID.name(), "CHAR(38)");
		access2HsqlTypesMap.put(AccessType.COUNTER.name(), "INTEGER");
		access2HsqlTypesMap.put(AccessType.NUMERIC.name(), "DECIMAL");
		access2HsqlTypesMap.put(AccessType.YESNO.name(), "BOOLEAN");
		access2HsqlTypesMap.put(AccessType.DATETIME.name(), "TIMESTAMP");
		access2HsqlTypesMap.put(AccessType.SINGLE.name(), "FLOAT");
		
		access2JackcessTypesMap.put(AccessType.BYTE, DataType.BYTE);
		access2JackcessTypesMap.put(AccessType.INTEGER, DataType.INT);
		access2JackcessTypesMap.put(AccessType.LONG, DataType.LONG); 
		access2JackcessTypesMap.put(AccessType.TEXT, DataType.TEXT);
		access2JackcessTypesMap.put(AccessType.OLE, DataType.OLE);
		access2JackcessTypesMap.put(AccessType.MEMO, DataType.MEMO );
		access2JackcessTypesMap.put(AccessType.CURRENCY, DataType.MONEY);
		access2JackcessTypesMap.put(AccessType.GUID, DataType.GUID);
		access2JackcessTypesMap.put(AccessType.COUNTER, DataType.LONG);
		access2JackcessTypesMap.put(AccessType.NUMERIC, DataType.NUMERIC);
		access2JackcessTypesMap.put(AccessType.YESNO, DataType.BOOLEAN);
		access2JackcessTypesMap.put(AccessType.DATETIME, DataType.SHORT_DATE_TIME);
		access2JackcessTypesMap.put(AccessType.SINGLE, DataType.FLOAT);
		access2JackcessTypesMap.put(AccessType.DOUBLE, DataType.DOUBLE);
		
		
		jackcess2HsqldbTypesMap.put( DataType.BYTE,"TINYINT");
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
