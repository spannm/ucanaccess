package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@RunWith(Parameterized.class)
public class InsertBigTest extends AccessVersionAllTest {

    public InsertBigTest(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Before
    public void beforeTestCase() throws Exception {
        executeStatements("CREATE TABLE Tbig (id LONG, descr MEMO)");
    }

    @Test
    public void testBig() throws SQLException, IOException {
        Statement st = null;
        st = ucanaccess.createStatement();
        int id = 6666554;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100000; i++) {
            sb.append(String.format("%05d", i));
            sb.append("\r\n");
        }
        String s = sb.toString();
        assertTrue(s.length() >= 65536);
        st.execute("INSERT INTO Tbig (id,descr)  VALUES( " + id + ",'" + s + "')");
        ResultSet rs = st.executeQuery("SELECT descr FROM Tbig WHERE id=" + id);
        rs.next();
        String retrieved = rs.getString(1);
        assertEquals(s, retrieved);
        st.close();
    }

}
