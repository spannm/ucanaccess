package net.ucanaccess.test.integration;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.ucanaccess.jdbc.UcanaccessConnection;
import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersion2010Test;

@RunWith(Parameterized.class)
public class Size97Test extends AccessVersion2010Test {

    public Size97Test(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Override
    public String getAccessPath() {
        return "testdbs/size97.mdb";
    }

    @Test
    public void testSize() throws Exception {
        Connection conn = getUcanaccessConnection();
        assertEquals("V1997", ((UcanaccessConnection) conn).getDbIO().getFileFormat().name());
        DatabaseMetaData dbmd = conn.getMetaData();
        ResultSet rs = dbmd.getColumns(null, null, "table1", "field1");
        rs.next();
        assertEquals(10, rs.getInt("COLUMN_SIZE"));
    }
}
