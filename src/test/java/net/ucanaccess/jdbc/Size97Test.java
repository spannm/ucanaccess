package net.ucanaccess.jdbc;

import static net.ucanaccess.type.SqlConstants.COLUMN_SIZE;

import net.ucanaccess.test.AccessVersionSource;
import net.ucanaccess.test.UcanaccessBaseFileTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

class Size97Test extends UcanaccessBaseFileTest {

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource(include = "V2010")
    void testSize(AccessVersion _accessVersion) throws Exception {
        init(_accessVersion);

        UcanaccessConnection conn = createUcanaccessConnection();
        assertEquals("V1997", conn.getDbIO().getFileFormat().name());
        DatabaseMetaData dbmd = conn.getMetaData();
        ResultSet rs = dbmd.getColumns(null, null, "table1", "field1");
        rs.next();
        assertEquals(10, rs.getInt(COLUMN_SIZE));
    }

}
