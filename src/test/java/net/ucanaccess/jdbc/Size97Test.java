package net.ucanaccess.jdbc;

import static net.ucanaccess.type.SqlConstants.COLUMN_SIZE;

import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

class Size97Test extends UcanaccessBaseTest {

    @Override
    protected String getAccessPath() {
        return getTestDbDir() + "size97.mdb";
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class, mode = Mode.INCLUDE, names = {"V2010"})
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
