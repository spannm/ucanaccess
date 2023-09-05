package net.ucanaccess.test.integration;

import net.ucanaccess.test.util.AccessVersion;
import net.ucanaccess.test.util.AccessVersionAllTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@RunWith(Parameterized.class)
public class GeneratedKeysTest1 extends AccessVersionAllTest {
    private String tableName = "T_Key1";

    public GeneratedKeysTest1(AccessVersion _accessVersion) {
        super(_accessVersion);
    }

    @Before
    public void beforeTestCase() throws Exception {
        executeStatements("CREATE TABLE " + tableName + " ( Z GUID PRIMARY KEY, B char(4) )");
    }

    @After
    public void afterTestCase() throws Exception {
        dropTable(tableName);
    }

    @Test
    public void testGeneratedKeys() throws SQLException, IOException {

        PreparedStatement ps = ucanaccess.prepareStatement("INSERT INTO " + tableName + " (B) VALUES (?)");
        ps.setString(1, "");
        ps.execute();
        ResultSet rs = ps.getGeneratedKeys();
        rs.next();
        PreparedStatement ps1 = ucanaccess.prepareStatement("Select @@identity ");
        ResultSet rs1 = ps1.executeQuery();
        rs1.next();
        assertEquals(rs1.getString(1), rs.getString(1));
        ps.close();

        checkQuery("SELECT * FROM " + tableName);

    }
}
