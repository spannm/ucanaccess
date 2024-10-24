package net.ucanaccess.jdbc;

import static net.ucanaccess.type.SqlConstants.COLUMN_SIZE;

import net.ucanaccess.test.UcanaccessBaseFileTest;
import org.junit.jupiter.api.Test;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

class Size97Test extends UcanaccessBaseFileTest {

    @Test
    void testSize() throws Exception {
        init();

        UcanaccessConnection conn = createUcanaccessConnection();
        assertEquals("V1997", conn.getDbIO().getFileFormat().name());
        DatabaseMetaData dbmd = conn.getMetaData();
        ResultSet rs = dbmd.getColumns(null, null, "table1", "field1");
        rs.next();
        assertEquals(10, rs.getInt(COLUMN_SIZE));
    }

}
