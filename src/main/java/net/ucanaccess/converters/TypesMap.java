package net.ucanaccess.converters;

import com.healthmarketscience.jackcess.DataType;
import net.ucanaccess.type.SqlConstants;

import java.util.*;
import java.util.stream.Collectors;

public final class TypesMap {

    public enum AccessType {
        AUTOINCREMENT(SqlConstants.INTEGER),
        BYTE(SqlConstants.SMALLINT),
        CHAR(SqlConstants.VARCHAR), // CHAR mapped into TEXT when used in CREATE TABLE.
        COMPLEX("OBJECT"),
        COUNTER(SqlConstants.INTEGER),
        CURRENCY("DECIMAL(" + DataType.MONEY.getFixedSize() + ", 4)"),
        DATETIME("TIMESTAMP"),
        DOUBLE("DOUBLE"),
        GUID("CHAR(38)"),
        HYPERLINK(SqlConstants.LONGVARCHAR), // HYPERLINK is a special type of MEMO field
        INTEGER("SMALLINT"),
        LONG(SqlConstants.INTEGER),
        MEMO(SqlConstants.LONGVARCHAR),
        NUMERIC("DECIMAL"),
        OLE("BLOB"),
        SINGLE("FLOAT"),
        TEXT(SqlConstants.VARCHAR),
        YESNO("BOOLEAN");

        private final String hsqlType;

        AccessType(String _hsqlType) {
            hsqlType = _hsqlType;
        }

        protected String getHsqlType() {
            return hsqlType;
        }

    }

    private static final Map<String, String>       ACCESS_TO_HSQL_TYPES_MAP     =
        Arrays.stream(AccessType.values()).collect(Collectors.toMap(AccessType::name, AccessType::getHsqlType, (o, n) -> o, LinkedHashMap::new));
    @SuppressWarnings("PMD.UseDiamondOperator")
    private static final Map<AccessType, DataType> ACCESS_TO_JACKCESS_TYPES_MAP = new EnumMap<AccessType, DataType>(AccessType.class);
    @SuppressWarnings("PMD.UseDiamondOperator")
    private static final Map<DataType, String>     JACKCESS_TO_HSQLDB_TYPES_MAP = new EnumMap<DataType, String>(DataType.class);

    static {
        ACCESS_TO_JACKCESS_TYPES_MAP.put(AccessType.AUTOINCREMENT, DataType.LONG);
        ACCESS_TO_JACKCESS_TYPES_MAP.put(AccessType.BYTE, DataType.BYTE);
        ACCESS_TO_JACKCESS_TYPES_MAP.put(AccessType.COUNTER, DataType.LONG);
        ACCESS_TO_JACKCESS_TYPES_MAP.put(AccessType.CURRENCY, DataType.MONEY);
        ACCESS_TO_JACKCESS_TYPES_MAP.put(AccessType.DATETIME, DataType.SHORT_DATE_TIME);
        ACCESS_TO_JACKCESS_TYPES_MAP.put(AccessType.DOUBLE, DataType.DOUBLE);
        ACCESS_TO_JACKCESS_TYPES_MAP.put(AccessType.GUID, DataType.GUID);
        ACCESS_TO_JACKCESS_TYPES_MAP.put(AccessType.HYPERLINK, DataType.MEMO);
        ACCESS_TO_JACKCESS_TYPES_MAP.put(AccessType.INTEGER, DataType.INT);
        ACCESS_TO_JACKCESS_TYPES_MAP.put(AccessType.LONG, DataType.LONG);
        ACCESS_TO_JACKCESS_TYPES_MAP.put(AccessType.MEMO, DataType.MEMO);
        ACCESS_TO_JACKCESS_TYPES_MAP.put(AccessType.NUMERIC, DataType.NUMERIC);
        ACCESS_TO_JACKCESS_TYPES_MAP.put(AccessType.OLE, DataType.OLE);
        ACCESS_TO_JACKCESS_TYPES_MAP.put(AccessType.SINGLE, DataType.FLOAT);
        ACCESS_TO_JACKCESS_TYPES_MAP.put(AccessType.TEXT, DataType.TEXT);
        ACCESS_TO_JACKCESS_TYPES_MAP.put(AccessType.YESNO, DataType.BOOLEAN);

        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.BIG_INT, "BIGINT");
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.BINARY, "BLOB");
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.BOOLEAN, "BOOLEAN");
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.BYTE, SqlConstants.SMALLINT);
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.COMPLEX_TYPE, "OBJECT");
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.DOUBLE, "DOUBLE");
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.FLOAT, "NUMERIC");
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.GUID, "CHAR(38)");
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.INT, SqlConstants.SMALLINT);
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.LONG, SqlConstants.INTEGER);
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.MEMO, SqlConstants.LONGVARCHAR);
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.MONEY, "DECIMAL(100,4)");
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.NUMERIC, "NUMERIC");
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.OLE, "BLOB");
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.SHORT_DATE_TIME, "TIMESTAMP");
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.TEXT, SqlConstants.VARCHAR);
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.UNKNOWN_0D, "BLOB");
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.UNKNOWN_11, "BLOB");
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.UNSUPPORTED_FIXEDLEN, "BLOB");
        JACKCESS_TO_HSQLDB_TYPES_MAP.put(DataType.UNSUPPORTED_VARLEN, "BLOB");
    }

    private TypesMap() {
    }

    public static Map<String, String> getAccess2HsqlTypesMap() {
        return Collections.unmodifiableMap(ACCESS_TO_HSQL_TYPES_MAP);
    }

    public static String map2hsqldb(DataType _type) {
        return JACKCESS_TO_HSQLDB_TYPES_MAP.getOrDefault(_type, _type.name());
    }

    public static DataType map2Jackcess(AccessType _type) {
        return ACCESS_TO_JACKCESS_TYPES_MAP.get(_type);
    }
}
