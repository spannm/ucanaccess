package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.UcanaccessTestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

class GeneratedKeys1Test extends UcanaccessTestBase {
    private String tableName = "T_Key";

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE " + tableName + " ( Z COUNTER PRIMARY KEY, B char(4) )");
    }

    @AfterEach
    void afterEachTest() throws SQLException {
        dropTable(tableName);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @EnumSource(value = AccessVersion.class)
    void testGeneratedKeys(AccessVersion _accessVersion) throws SQLException, IOException {
        init(_accessVersion);

        PreparedStatement ps = ucanaccess.prepareStatement("INSERT INTO " + tableName + " (B) VALUES (?)");
        ps.setString(1, "");
        ps.execute();
        ResultSet rs = ps.getGeneratedKeys();
        rs.next();
        assertEquals(1, rs.getInt(1));
        ps.close();

        ps = ucanaccess.prepareStatement("Select @@identity ");
        rs = ps.executeQuery();
        rs.next();
        assertEquals(1, rs.getInt(1));
        Statement st = ucanaccess.createStatement();
        st.execute("INSERT INTO " + tableName + " (B) VALUES ('W')");

        checkQuery("Select @@identity ", 2);
        rs = st.getGeneratedKeys();
        rs.next();
        assertEquals(2, rs.getInt(1));

    }
}
