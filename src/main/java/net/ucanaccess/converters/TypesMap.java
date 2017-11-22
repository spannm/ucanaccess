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

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import com.healthmarketscience.jackcess.DataType;

public final class TypesMap {
    public static enum AccessType {
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
        COMPLEX,
        CHAR,
        HYPERLINK
    }

    private static final Map<String, String>       ACCESS_TO_HSQL_TYPES_MAP     = new LinkedHashMap<String, String>();
    private static final Map<AccessType, DataType> ACCESS_TO_JACKCESS_TYPES_MAP = new HashMap<AccessType, DataType>();
    private static final Map<DataType, String>     JACKCESS_TO_HSQLDB_TYPES_MAP = new HashMap<DataType, String>();

    static {
        ACCESS_TO_HSQL_TYPES_MAP.put(AccessType.BYTE.name(), "SMALLINT");
        ACCESS_TO_HSQL_TYPES_MAP.put(AccessType.INTEGER.name(), "SMALLINT");
        ACCESS_TO_HSQL_TYPES_MAP.put(AccessType.LONG.name(), "INTEGER");
        ACCESS_TO_HSQL_TYPES_MAP.put(AccessType.TEXT.name(), "VARCHAR");
        ACCESS_TO_HSQL_TYPES_MAP.put(AccessType.OLE.name(), "BLOB");
        ACCESS_TO_HSQL_TYPES_MAP.put(AccessType.MEMO.name(), "LONGVARCHAR");
        ACCESS_TO_HSQL_TYPES_MAP.put(AccessType.CURRENCY.name(), "DECIMAL(" + DataType.MONEY.getFixedSize() + ",4)");
        ACCESS_TO_HSQL_TYPES_MAP.put(AccessType.GUID.name(), "CHAR(38)");
        ACCESS_TO_HSQL_TYPES_MAP.put(AccessType.COUNTER.name(), "INTEGER");
        ACCESS_TO_HSQL_TYPES_MAP.put(AccessType.AUTOINCREMENT.name(), "INTEGER");
        ACCESS_TO_HSQL_TYPES_MAP.put(AccessType.NUMERIC.name(), "DECIMAL");
        ACCESS_TO_HSQL_TYPES_MAP.put(AccessType.YESNO.name(), "BOOLEAN");
        ACCESS_TO_HSQL_TYPES_MAP.put(AccessType.DATETIME.name(), "TIMESTAMP");
        ACCESS_TO_HSQL_TYPES_MAP.put(AccessType.SINGLE.name(), "FLOAT");
        ACCESS_TO_HSQL_TYPES_MAP.put(AccessType.COMPLEX.name(), "OBJECT");
        ACCESS_TO_HSQL_TYPES_MAP.put(AccessType.CHAR.name(), "VARCHAR"); // CHAR mapped into TEXT when used in CREATE TABLE.
        ACCESS_TO_HSQL_TYPES_MAP.put(AccessType.HYPERLINK.name(), "LONGVARCHAR"); // HYPERLINK is a special type of MEMO
                                                                             // field

        ACCESS_TO_JACKCESS_TYPES_MAP.put(AccessType.BYTE, DataType.BYTE);
        ACCESS_TO_JACKCESS_TYPES_MAP.put(AccessType.INTEGER, DataType.INT);
        ACCESS_TO_JACKCESS_TYPES_MAP.put(AccessType.LONG, DataType.LONG);
        ACCESS_TO_JACKCESS_TYPES_MAP.put(AccessType.TEXT, DataType.TEXT);
        ACCESS_TO_JACKCESS_TYPES_MAP.put(AccessType.OLE, DataType.OLE);
        ACCESS_TO_JACKCESS_TYPES_MAP.put(AccessType.MEMO, DataType.MEMO);
        ACCESS_TO_JACKCESS_TYPES_MAP.put(AccessType.CURRENCY, DataType.MONEY);
        ACCESS_TO_JACKCESS_TYPES_MAP.put(AccessType.GUID, DataType.GUID);
        ACCESS_TO_JACKCESS_TYPES_MAP.put(AccessType.COUNTER, DataType.LONG);
        ACCESS_TO_JACKCESS_TYPES_MAP.put(AccessType.AUTOINCREMENT, DataType.LONG);
        ACCESS_TO_JACKCESS_TYPES_MAP.put(AccessType.NUMERIC, DataType.NUMERIC);
        ACCESS_TO_JACKCESS_TYPES_MAP.put(AccessType.YESNO, DataType.BOOLEAN);
        ACCESS_TO_JACKCESS_TYPES_MAP.put(AccessType.DATETIME, DataType.SHORT_DATE_TIME);
        ACCESS_TO_JACKCESS_TYPES_MAP.put(AccessType.SINGLE, DataType.FLOAT);
        ACCESS_TO_JACKCESS_TYPES_MAP.put(AccessType.DOUBLE, DataType.DOUBLE);
        ACCESS_TO_JACKCESS_TYPES_MAP.put(AccessType.HYPERLINK, DataType.MEMO);

        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.BYTE, "SMALLINT");
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.INT, "SMALLINT");
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.LONG, "INTEGER");
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.TEXT, "VARCHAR");
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.BINARY, "BLOB");
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.MEMO, "LONGVARCHAR");
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.MONEY, "DECIMAL(100,4)");
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.GUID, "CHAR(38)");
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.OLE, "BLOB");
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.NUMERIC, "NUMERIC");
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.BOOLEAN, "BOOLEAN");
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.SHORT_DATE_TIME, "TIMESTAMP");
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.FLOAT, "FLOAT");
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.DOUBLE, "DOUBLE");
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.COMPLEX_TYPE, "OBJECT");
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.UNKNOWN_11, "BLOB");
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.UNKNOWN_0D, "BLOB");
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.UNSUPPORTED_FIXEDLEN, "BLOB");
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.UNSUPPORTED_VARLEN, "BLOB");
    }

    private TypesMap() {
    }

    public static Map<String, String> getAccess2HsqlTypesMap() {
        return Collections.unmodifiableMap(ACCESS_TO_HSQL_TYPES_MAP);
    }

    public static String map2hsqldb(DataType _type) {
        if (JACKCESS_TO_HSQLDB_TYPES_MAP.containsKey(_type)) {
            return JACKCESS_TO_HSQLDB_TYPES_MAP.get(_type);
        }
        return _type.name();
    }

    public static DataType map2Jackcess(AccessType _type) {
        return ACCESS_TO_JACKCESS_TYPES_MAP.get(_type);
    }
}
