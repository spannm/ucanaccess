package net.ucanaccess.jdbc;

import net.ucanaccess.test.AccessVersionSource;
import net.ucanaccess.test.UcanaccessBaseTest;
import net.ucanaccess.type.AccessVersion;
import org.junit.jupiter.params.ParameterizedTest;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

class GeneratedKeys1Test extends UcanaccessBaseTest {
    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE t_key (Z COUNTER PRIMARY KEY, B CHAR(4))");
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @AccessVersionSource
    void testGeneratedKeys(AccessVersion _accessVersion) throws SQLException {
        init(_accessVersion);

        try (PreparedStatement ps = ucanaccess.prepareStatement("INSERT INTO t_key (B) VALUES (?)")) {
            ps.setString(1, "");
            ps.execute();
            ResultSet rs = ps.getGeneratedKeys();
            rs.next();
            assertEquals(1, rs.getInt(1));
        }

        try (PreparedStatement ps2 = ucanaccess.prepareStatement("SELECT @@identity")) {
            ResultSet rs2 = ps2.executeQuery();
            rs2.next();
            assertEquals(1, rs2.getInt(1));
            UcanaccessStatement st = ucanaccess.createStatement();
            st.execute("INSERT INTO t_key (B) VALUES ('W')");

            checkQuery("SELECT @@identity", singleRec(2));
            ResultSet rs3 = st.getGeneratedKeys();
            rs3.next();
            assertEquals(2, rs3.getInt(1));
        }
    }
}
