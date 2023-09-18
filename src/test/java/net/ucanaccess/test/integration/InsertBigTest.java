package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessBaseTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

class InsertBigTest extends UcanaccessBaseTest {

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE Tbig (id LONG, descr MEMO)");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testBig(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);
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
