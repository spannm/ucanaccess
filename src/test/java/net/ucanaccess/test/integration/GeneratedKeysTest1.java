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

public class GeneratedKeysTest1 extends UcanaccessTestBase {
    private String tableName = "T_Key1";

    @Override
    protected void init(AccessVersion _accessVersion) throws SQLException {
        super.init(_accessVersion);
        executeStatements("CREATE TABLE " + tableName + " ( Z GUID PRIMARY KEY, B char(4) )");
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
        PreparedStatement ps1 = ucanaccess.prepareStatement("Select @@identity ");
        ResultSet rs1 = ps1.executeQuery();
        rs1.next();
        assertEquals(rs1.getString(1), rs.getString(1));
        ps.close();

        checkQuery("SELECT * FROM " + tableName);

    }
}
